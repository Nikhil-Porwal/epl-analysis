
"""@bruin
name: "ingest_kaggle"
type: "python"
description: "Validates and profiles the raw Kaggle EPL CSV before Spark transform"
parameters: {
    seasons: "2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24,2024/25"
}
@bruin"""

import os
import csv
import logging
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict

# ── Setup ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingest_kaggle.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RAW_PATH     = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
SOURCE_FILE  = RAW_PATH / "epl_final.csv"
SEASONS      = os.getenv(
    "SEASONS",
    "2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24,2024/25"
).split(",")

REQUIRED_COLUMNS = [
    "Season", "MatchDate", "HomeTeam", "AwayTeam",
    "FullTimeHomeGoals", "FullTimeAwayGoals", "FullTimeResult",
    "HalfTimeHomeGoals", "HalfTimeAwayGoals", "HalfTimeResult",
    "HomeShots", "AwayShots", "HomeShotsOnTarget", "AwayShotsOnTarget",
    "HomeCorners", "AwayCorners", "HomeFouls", "AwayFouls",
    "HomeYellowCards", "AwayYellowCards", "HomeRedCards", "AwayRedCards",
]


def validate_source() -> None:
    """Check source file exists and has required columns."""
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}\n"
            f"Download from Kaggle and place in {RAW_PATH}/"
        )
    log.info(f"Source file found → {SOURCE_FILE}")

    with open(SOURCE_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        actual_cols = reader.fieldnames or []

    missing = [c for c in REQUIRED_COLUMNS if c not in actual_cols]
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")
    log.info(f"All {len(REQUIRED_COLUMNS)} required columns present ✓")


def profile_data() -> dict:
    """Profile the CSV — row counts, seasons, nulls, date range."""
    log.info("Profiling raw data...")

    season_counts   = defaultdict(int)
    result_counts   = defaultdict(int)
    null_counts     = defaultdict(int)
    total_rows      = 0
    min_date        = "9999-99-99"
    max_date        = "0000-00-00"

    with open(SOURCE_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            season = row.get("Season", "").strip()
            result = row.get("FullTimeResult", "").strip()
            date   = row.get("MatchDate", "").strip()

            season_counts[season] += 1
            result_counts[result] += 1

            if date and date < min_date:
                min_date = date
            if date and date > max_date:
                max_date = date

            for col in REQUIRED_COLUMNS:
                if not row.get(col, "").strip():
                    null_counts[col] += 1

    profile = {
        "total_rows":    total_rows,
        "total_seasons": len(season_counts),
        "season_counts": dict(season_counts),
        "result_counts": dict(result_counts),
        "null_counts":   dict(null_counts),
        "date_range":    f"{min_date} → {max_date}",
    }
    return profile


def filter_seasons(profile: dict) -> dict:
    """Check configured seasons are present in the data."""
    available = set(profile["season_counts"].keys())
    configured = set(SEASONS)
    found    = configured & available
    missing  = configured - available

    log.info(f"Configured seasons: {len(configured)}")
    log.info(f"Found in data:      {len(found)} → {sorted(found)}")

    if missing:
        log.warning(f"Missing seasons:    {sorted(missing)}")

    total_filtered = sum(
        profile["season_counts"].get(s, 0) for s in found
    )
    log.info(f"Total matches for configured seasons: {total_filtered}")
    return {"found": sorted(found), "missing": sorted(missing), "total_matches": total_filtered}


def print_summary(profile: dict, season_check: dict) -> None:
    """Print a clean summary table."""
    print("\n── Ingest Profile ────────────────────────────────────────")
    print(f"  Source file    : {SOURCE_FILE}")
    print(f"  Total rows     : {profile['total_rows']:,}")
    print(f"  Total seasons  : {profile['total_seasons']}")
    print(f"  Date range     : {profile['date_range']}")
    print(f"  Results (H/D/A): {profile['result_counts']}")
    print()
    print("  Seasons in pipeline:")
    for s in season_check["found"]:
        count = profile["season_counts"].get(s, 0)
        print(f"    {s}  →  {count} matches")
    print()
    if any(v > 0 for v in profile["null_counts"].values()):
        print("  Null counts:")
        for col, count in profile["null_counts"].items():
            if count > 0:
                print(f"    {col:<30} {count}")
    else:
        print("  Null counts    : none ✓")
    print(f"\n  Pipeline matches: {season_check['total_matches']:,}")
    print("──────────────────────────────────────────────────────────\n")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting Kaggle EPL data ingestion & validation...")

    validate_source()
    profile      = profile_data()
    season_check = filter_seasons(profile)
    print_summary(profile, season_check)

    log.info("Ingest validation complete ✓ — ready for Spark transform")