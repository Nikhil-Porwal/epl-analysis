"""@bruin
name: "load_snowflake.py"
description: "Loads Parquet files into Snowflake EPL_DB.RAW.matches"
type: "python"
depends_on: ["spark_transform"]
@bruin"""

import os
import glob
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
 
# ── Setup ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/load_snowflake.log"),
    ],
)
log = logging.getLogger(__name__)
 
# ── Config ────────────────────────────────────────────────────────────────────
STAGING_PATH  = Path(os.getenv("STAGING_DATA_PATH", "data/staging"))
PARQUET_DIR   = STAGING_PATH / "epl_matches.parquet"
 
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "EPL_DB")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "EPL_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "ACCOUNTADMIN")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW")
TABLE_NAME          = "MATCHES"
 
 
# ── Helpers ───────────────────────────────────────────────────────────────────
def get_connection():
    log.info(f"Connecting to Snowflake → {SNOWFLAKE_ACCOUNT} / {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    log.info("Connected ✓")
    return conn
 
 
def read_parquet_partitions() -> pd.DataFrame:
    """Read all season partitions into a single DataFrame."""
    pattern = str(PARQUET_DIR / "season_start_year=*" / "*.parquet")
    files   = glob.glob(pattern)
 
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found at {pattern}\n"
            "Run spark_transform.py first."
        )
 
    log.info(f"Found {len(files)} Parquet partition(s)")
    dfs = []
 
    for f in sorted(files):
        season_year = Path(f).parent.name.split("=")[1]
        df = pd.read_parquet(f)
        df["season_start_year"] = int(season_year)  # restore partition column
        dfs.append(df)
        log.info(f"  Loaded partition {season_year} → {len(df):,} rows")
 
    combined = pd.concat(dfs, ignore_index=True)
    log.info(f"Total rows loaded: {len(combined):,}")
    return combined
 
 
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase all column names for Snowflake compatibility."""
    df.columns = [c.upper() for c in df.columns]
 
    # Convert timestamp columns to string (Snowflake connector handles better)
    if "INGESTED_AT" in df.columns:
        df["INGESTED_AT"] = df["INGESTED_AT"].astype(str)
 
    # Convert date column to string
    if "MATCH_DATE" in df.columns:
        df["MATCH_DATE"] = df["MATCH_DATE"].astype(str)
 
    return df
 
 
def create_table_if_not_exists(conn) -> None:
    """Create RAW.MATCHES table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME} (
        MATCH_DATE          DATE,
        HOME_TEAM           VARCHAR(100),
        AWAY_TEAM           VARCHAR(100),
        HOME_GOALS          INTEGER,
        AWAY_GOALS          INTEGER,
        TOTAL_GOALS         INTEGER,
        RESULT              VARCHAR(1),
        HOME_WIN            INTEGER,
        AWAY_WIN            INTEGER,
        DRAW                INTEGER,
        HOME_POINTS         INTEGER,
        AWAY_POINTS         INTEGER,
        HT_HOME_GOALS       INTEGER,
        HT_AWAY_GOALS       INTEGER,
        HT_RESULT           VARCHAR(1),
        HOME_SHOTS          INTEGER,
        AWAY_SHOTS          INTEGER,
        HOME_SHOTS_ON_TARGET INTEGER,
        AWAY_SHOTS_ON_TARGET INTEGER,
        HOME_YELLOW_CARDS   INTEGER,
        AWAY_YELLOW_CARDS   INTEGER,
        HOME_RED_CARDS      INTEGER,
        AWAY_RED_CARDS      INTEGER,
        HOME_CORNERS        INTEGER,
        AWAY_CORNERS        INTEGER,
        HOME_FOULS          INTEGER,
        AWAY_FOULS          INTEGER,
        SEASON              VARCHAR(10),
        SEASON_START_YEAR   INTEGER,
        INGESTED_AT         VARCHAR(50)
    );
    """
    cursor = conn.cursor()
    cursor.execute(ddl)
    cursor.close()
    log.info(f"Table {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME} ready ✓")
 
 
def truncate_table(conn) -> None:
    """Truncate before reload — ensures idempotent runs."""
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}")
    cursor.close()
    log.info(f"Table truncated — fresh load ✓")
 
 
def load_to_snowflake(conn, df: pd.DataFrame) -> None:
    """Load DataFrame into Snowflake using write_pandas."""
    log.info(f"Loading {len(df):,} rows into {SNOWFLAKE_SCHEMA}.{TABLE_NAME}...")
 
    success, chunks, rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=TABLE_NAME,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
    )
 
    if success:
        log.info(f"Load complete ✓ → {rows:,} rows in {chunks} chunk(s)")
    else:
        raise RuntimeError(f"Snowflake load failed: {output}")
 
 
def verify_load(conn) -> None:
    """Quick row count verification after load."""
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COUNT(*), MIN(MATCH_DATE), MAX(MATCH_DATE), COUNT(DISTINCT SEASON) "
        f"FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}"
    )
    row = cursor.fetchone()
    cursor.close()
 
    print("\n── Load Verification ──────────────────────────────────")
    print(f"  Rows loaded    : {row[0]:,}")
    print(f"  Date range     : {row[1]} → {row[2]}")
    print(f"  Seasons        : {row[3]}")
    print(f"  Table          : {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}")
    print("────────────────────────────────────────────────────────\n")
 
 
# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting Snowflake load...")
 
    # 1. Read all Parquet partitions
    df = read_parquet_partitions()
    df = normalize_columns(df)
 
    log.info(f"Columns: {list(df.columns)}")
    log.info(f"Shape: {df.shape}")
 
    # 2. Connect and prepare table
    conn = get_connection()
    create_table_if_not_exists(conn)
    truncate_table(conn)
 
    # 3. Load
    load_to_snowflake(conn, df)
 
    # 4. Verify
    verify_load(conn)
 
    conn.close()
    log.info("Snowflake load complete ✓ — ready for dbt")
