"""@bruin
name: "run_dbt"
type: "python"
description: "Runs all dbt models — staging, intermediate, marts"
depends_on: ["load_snowflake"]
@bruin"""
import subprocess
import logging
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


def run_dbt():
    log.info(f"Running dbt from {DBT_PROJECT_DIR}")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR)],
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
        ["dbt", "test", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        log.warning(f"Some dbt tests failed:\n{result.stderr}")
    else:
        log.info("All dbt tests passed ✓")


if __name__ == "__main__":
    run_dbt()
    run_dbt_tests()
