"""@bruin
name: "run_dbt"
type: "python"
description: "Runs all dbt models — staging, intermediate, marts"
depends_on: ["load_snowflake"]
@bruin"""
import subprocess
import logging
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/run_dbt.log"),
    ],
)
log = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).parents[2] / "epl_dbt"
DBT_PROFILES_DIR = Path("/home/nikhilporwal/.dbt")


def wait_for_snowflake_data(max_wait_seconds=120, poll_interval=5):
    """Poll RAW.MATCHES until rows appear, then return."""
    import snowflake.connector
    import os

    log.info("Waiting for Snowflake RAW.MATCHES to be populated...")

    # Reuse creds from your existing env/config
    conn_kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="EPL_DB",
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )

    elapsed = 0
    while elapsed < max_wait_seconds:
        try:
            with snowflake.connector.connect(**conn_kwargs) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM EPL_DB.RAW.MATCHES")
                row_count = cur.fetchone()[0]
                if row_count > 0:
                    log.info(f"RAW.MATCHES has {row_count:,} rows — proceeding with dbt ✓")
                    return
                else:
                    log.info(f"RAW.MATCHES empty, retrying in {poll_interval}s... ({elapsed}s elapsed)")
        except Exception as e:
            log.warning(f"Snowflake check failed: {e} — retrying in {poll_interval}s...")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise RuntimeError(f"RAW.MATCHES still empty after {max_wait_seconds}s — aborting dbt run")


def run_dbt():
    log.info(f"Running dbt from {DBT_PROJECT_DIR}")

    result = subprocess.run(
        [
            "dbt", "run",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
        ],
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")

    log.info("dbt run complete ✓")


def run_dbt_tests():
    log.info("Running dbt tests...")

    result = subprocess.run(
        [
            "dbt", "test",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
        ],
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        log.warning(f"Some dbt tests failed:\n{result.stderr}")
    else:
        log.info("All dbt tests passed ✓")


if __name__ == "__main__":
    wait_for_snowflake_data()
    run_dbt()
    run_dbt_tests()