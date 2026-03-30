"""@bruin
name: "spark_transform"
type: "python"
description: "PySpark job: reads raw EPL CSV, filters seasons, cleans and writes Parquet"
depends_on: ["ingest_kaggle"]
@bruin"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType
)

# ── Setup ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/spark_transform.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RAW_PATH      = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
STAGING_PATH  = Path(os.getenv("STAGING_DATA_PATH", "data/staging"))
SOURCE_FILE   = str(RAW_PATH / "epl_final.csv")
OUTPUT_FILE   = str(STAGING_PATH / "epl_matches.parquet")
SEASONS       = os.getenv(
    "SEASONS",
    "2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24,2024/25"
).split(",")


# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("Season",               StringType(),  True),
    StructField("MatchDate",            StringType(),  True),
    StructField("HomeTeam",             StringType(),  True),
    StructField("AwayTeam",             StringType(),  True),
    StructField("FullTimeHomeGoals",    IntegerType(), True),
    StructField("FullTimeAwayGoals",    IntegerType(), True),
    StructField("FullTimeResult",       StringType(),  True),
    StructField("HalfTimeHomeGoals",    IntegerType(), True),
    StructField("HalfTimeAwayGoals",    IntegerType(), True),
    StructField("HalfTimeResult",       StringType(),  True),
    StructField("HomeShots",            IntegerType(), True),
    StructField("AwayShots",            IntegerType(), True),
    StructField("HomeShotsOnTarget",    IntegerType(), True),
    StructField("AwayShotsOnTarget",    IntegerType(), True),
    StructField("HomeCorners",          IntegerType(), True),
    StructField("AwayCorners",          IntegerType(), True),
    StructField("HomeFouls",            IntegerType(), True),
    StructField("AwayFouls",            IntegerType(), True),
    StructField("HomeYellowCards",      IntegerType(), True),
    StructField("AwayYellowCards",      IntegerType(), True),
    StructField("HomeRedCards",         IntegerType(), True),
    StructField("AwayRedCards",         IntegerType(), True),
])


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EPL-Transform")
        .config("spark.sql.shuffle.partitions", "4")   # small data → fewer partitions
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")            # disable UI to save memory
        .master("local[*]")
        .getOrCreate()
    )


def read_raw(spark: SparkSession):
    log.info(f"Reading raw CSV → {SOURCE_FILE}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(SCHEMA)
        .csv(SOURCE_FILE)
    )
    log.info(f"Raw rows: {df.count():,}")
    return df


def filter_seasons(df):
    # Support either full season labels (e.g. 2015/16) or start years (e.g. 2015).
    requested = [s.strip() for s in SEASONS if s and s.strip()]
    exact_labels = [s for s in requested if "/" in s]
    start_years = [s for s in requested if "/" not in s]

    condition = F.lit(False)
    if exact_labels:
        condition = condition | F.col("Season").isin(exact_labels)
    if start_years:
        condition = condition | F.split(F.col("Season"), "/").getItem(0).isin(start_years)

    log.info(f"Filtering to {len(requested)} seasons: {requested}")
    df = df.filter(condition)
    log.info(f"Rows after season filter: {df.count():,}")
    return df


def clean_and_enrich(df):
    log.info("Cleaning and enriching data...")
    df = (
        df
        # Parse match date
        .withColumn("match_date", F.to_date("MatchDate", "yyyy-MM-dd"))

        # Normalize team names (trim whitespace)
        .withColumn("home_team", F.trim(F.col("HomeTeam")))
        .withColumn("away_team", F.trim(F.col("AwayTeam")))

        # Goals
        .withColumn("home_goals", F.col("FullTimeHomeGoals"))
        .withColumn("away_goals", F.col("FullTimeAwayGoals"))
        .withColumn("total_goals", F.col("FullTimeHomeGoals") + F.col("FullTimeAwayGoals"))

        # Result flags
        .withColumn("result",      F.col("FullTimeResult"))   # H / A / D
        .withColumn("home_win",    F.when(F.col("FullTimeResult") == "H", 1).otherwise(0))
        .withColumn("away_win",    F.when(F.col("FullTimeResult") == "A", 1).otherwise(0))
        .withColumn("draw",        F.when(F.col("FullTimeResult") == "D", 1).otherwise(0))

        # Points (for league table calc in dbt)
        .withColumn("home_points",
            F.when(F.col("FullTimeResult") == "H", 3)
             .when(F.col("FullTimeResult") == "D", 1)
             .otherwise(0))
        .withColumn("away_points",
            F.when(F.col("FullTimeResult") == "A", 3)
             .when(F.col("FullTimeResult") == "D", 1)
             .otherwise(0))

        # Half time
        .withColumn("ht_home_goals", F.col("HalfTimeHomeGoals"))
        .withColumn("ht_away_goals", F.col("HalfTimeAwayGoals"))
        .withColumn("ht_result",     F.col("HalfTimeResult"))

        # Shots
        .withColumn("home_shots",            F.col("HomeShots"))
        .withColumn("away_shots",            F.col("AwayShots"))
        .withColumn("home_shots_on_target",  F.col("HomeShotsOnTarget"))
        .withColumn("away_shots_on_target",  F.col("AwayShotsOnTarget"))

        # Discipline
        .withColumn("home_yellow_cards", F.col("HomeYellowCards"))
        .withColumn("away_yellow_cards", F.col("AwayYellowCards"))
        .withColumn("home_red_cards",    F.col("HomeRedCards"))
        .withColumn("away_red_cards",    F.col("AwayRedCards"))

        # Corners & fouls
        .withColumn("home_corners", F.col("HomeCorners"))
        .withColumn("away_corners", F.col("AwayCorners"))
        .withColumn("home_fouls",   F.col("HomeFouls"))
        .withColumn("away_fouls",   F.col("AwayFouls"))

        # Season year (for partitioning & trends)
        .withColumn("season_start_year",
            F.split(F.col("Season"), "/").getItem(0).cast("integer"))

        # Metadata
        .withColumn("ingested_at", F.current_timestamp())

        # Drop original raw columns
        .drop(
            "MatchDate", "HomeTeam", "AwayTeam",
            "FullTimeHomeGoals", "FullTimeAwayGoals", "FullTimeResult",
            "HalfTimeHomeGoals", "HalfTimeAwayGoals", "HalfTimeResult",
            "HomeShots", "AwayShots", "HomeShotsOnTarget", "AwayShotsOnTarget",
            "HomeCorners", "AwayCorners", "HomeFouls", "AwayFouls",
            "HomeYellowCards", "AwayYellowCards", "HomeRedCards", "AwayRedCards",
        )
        .withColumnRenamed("Season", "season")
    )

    # Drop rows with null critical fields
    before = df.count()
    df = df.dropna(subset=["match_date", "home_team", "away_team", "home_goals", "away_goals"])
    after = df.count()
    if before != after:
        log.warning(f"Dropped {before - after} rows with null critical fields")

    return df


def write_parquet(df) -> None:
    STAGING_PATH.mkdir(parents=True, exist_ok=True)
    log.info(f"Writing Parquet → {OUTPUT_FILE}")
    (
        df
        .repartition(1)                   # single file — small data
        .write
        .mode("overwrite")
        .partitionBy("season_start_year")
        .parquet(OUTPUT_FILE)
    )
    log.info("Parquet written ✓")


def print_summary(df) -> None:
    print("\n── Transform Summary ──────────────────────────────────────")
    print(f"  Output path : {OUTPUT_FILE}")
    print(f"  Total rows  : {df.count():,}")
    print()
    print("  Rows per season:")
    (
        df.groupBy("season")
          .count()
          .orderBy("season")
          .show(truncate=False)
    )
    print()
    print("  Sample columns:")
    df.select(
        "season", "match_date", "home_team", "away_team",
        "home_goals", "away_goals", "result", "total_goals"
    ).show(5, truncate=False)
    print("──────────────────────────────────────────────────────────\n")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting PySpark EPL transform...")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = read_raw(spark)
    df = filter_seasons(df)
    df = clean_and_enrich(df)
    write_parquet(df)
    print_summary(df)

    spark.stop()
    log.info("Spark transform complete ✓ — ready for Snowflake load")