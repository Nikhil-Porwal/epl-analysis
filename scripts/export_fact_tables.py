"""Build the dashboard fact tables from the local Parquet data."""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = PROJECT_ROOT / "data" / "staging" / "epl_matches.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data" / "facts"


def read_matches() -> pd.DataFrame:
    files = sorted(PARQUET_DIR.glob("season_start_year=*/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet partitions found in {PARQUET_DIR}. "
            "Run the Spark transform first."
        )

    frames = []
    for file_path in files:
        season_start_year = int(file_path.parent.name.split("=")[1])
        frame = pd.read_parquet(file_path)
        frame["season_start_year"] = season_start_year
        frames.append(frame)

    matches = pd.concat(frames, ignore_index=True)
    matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
    matches = matches.dropna(
        subset=["match_date", "home_team", "away_team", "home_goals", "away_goals"]
    )
    return matches


def build_league_table(matches: pd.DataFrame) -> pd.DataFrame:
    home = pd.DataFrame(
        {
            "season": matches["season"],
            "season_start_year": matches["season_start_year"],
            "team": matches["home_team"],
            "matches_played": 1,
            "wins": matches["home_win"],
            "draws": matches["draw"],
            "losses": matches["away_win"],
            "goals_scored": matches["home_goals"],
            "goals_conceded": matches["away_goals"],
            "points": matches["home_points"],
            "yellow_cards": matches["home_yellow_cards"],
            "red_cards": matches["home_red_cards"],
            "shots": matches["home_shots"],
            "shots_on_target": matches["home_shots_on_target"],
        }
    )
    away = pd.DataFrame(
        {
            "season": matches["season"],
            "season_start_year": matches["season_start_year"],
            "team": matches["away_team"],
            "matches_played": 1,
            "wins": matches["away_win"],
            "draws": matches["draw"],
            "losses": matches["home_win"],
            "goals_scored": matches["away_goals"],
            "goals_conceded": matches["home_goals"],
            "points": matches["away_points"],
            "yellow_cards": matches["away_yellow_cards"],
            "red_cards": matches["away_red_cards"],
            "shots": matches["away_shots"],
            "shots_on_target": matches["away_shots_on_target"],
        }
    )

    team_stats = pd.concat([home, away], ignore_index=True)
    totals = (
        team_stats.groupby(["season", "season_start_year", "team"], as_index=False)
        .sum(numeric_only=True)
    )
    totals["goal_difference"] = (
        totals["goals_scored"] - totals["goals_conceded"]
    )
    totals["win_rate_pct"] = (
        totals["wins"] / totals["matches_played"] * 100
    ).round(1)
    totals["draw_rate_pct"] = (
        totals["draws"] / totals["matches_played"] * 100
    ).round(1)
    totals["loss_rate_pct"] = (
        totals["losses"] / totals["matches_played"] * 100
    ).round(1)
    totals["avg_goals_scored"] = (
        totals["goals_scored"] / totals["matches_played"]
    ).round(2)
    totals["avg_goals_conceded"] = (
        totals["goals_conceded"] / totals["matches_played"]
    ).round(2)
    totals["shot_accuracy_pct"] = (
        totals["shots_on_target"] / totals["shots"] * 100
    ).round(1)

    totals = totals.sort_values(
        ["season", "points", "goal_difference", "goals_scored"],
        ascending=[True, False, False, False],
    )
    totals["season_rank"] = totals.groupby("season").cumcount() + 1
    totals["top_4"] = totals["season_rank"] <= 4
    totals["relegated"] = totals["season_rank"] >= 18
    return totals.sort_values(["season", "season_rank"]).reset_index(drop=True)


def build_match_kpi(matches: pd.DataFrame) -> pd.DataFrame:
    grouped = matches.groupby(["season", "season_start_year"], as_index=False)
    kpi = grouped.agg(
        total_matches=("season", "size"),
        total_goals=("total_goals", "sum"),
        avg_goals_per_match=("total_goals", "mean"),
        avg_home_goals=("home_goals", "mean"),
        avg_away_goals=("away_goals", "mean"),
        avg_ht_goals=("ht_home_goals", lambda values: values.mean()),
        home_wins=("home_win", "sum"),
        away_wins=("away_win", "sum"),
        draws=("draw", "sum"),
        avg_home_shots=("home_shots", "mean"),
        avg_away_shots=("away_shots", "mean"),
        avg_home_sot=("home_shots_on_target", "mean"),
        avg_away_sot=("away_shots_on_target", "mean"),
        avg_home_corners=("home_corners", "mean"),
        avg_away_corners=("away_corners", "mean"),
    )
    kpi["avg_ht_goals"] = (
        matches.assign(ht_goals=matches["ht_home_goals"] + matches["ht_away_goals"])
        .groupby(["season", "season_start_year"])["ht_goals"]
        .mean()
        .round(2)
        .to_numpy()
    )
    kpi["home_win_pct"] = (kpi["home_wins"] / kpi["total_matches"] * 100).round(1)
    kpi["away_win_pct"] = (kpi["away_wins"] / kpi["total_matches"] * 100).round(1)
    kpi["draw_pct"] = (kpi["draws"] / kpi["total_matches"] * 100).round(1)
    kpi["home_advantage_index"] = (
        kpi["home_wins"] / kpi["away_wins"].replace(0, pd.NA)
    ).round(2)
    kpi["high_scoring_matches"] = (
        matches.assign(high_scoring=matches["total_goals"] >= 5)
        .groupby(["season", "season_start_year"])["high_scoring"]
        .sum()
        .to_numpy()
    )
    kpi["high_scoring_pct"] = (
        kpi["high_scoring_matches"] / kpi["total_matches"] * 100
    ).round(1)
    kpi["comeback_wins"] = (
        (
            ((matches["ht_result"] == "A") & (matches["result"] == "H"))
            | ((matches["ht_result"] == "H") & (matches["result"] == "A"))
        )
        .groupby([matches["season"], matches["season_start_year"]])
        .sum()
        .to_numpy()
    )

    rounded_columns = [
        "avg_home_shots",
        "avg_away_shots",
        "avg_home_sot",
        "avg_away_sot",
        "avg_home_corners",
        "avg_away_corners",
    ]
    kpi[rounded_columns] = kpi[rounded_columns].round(1)
    kpi[
        ["avg_goals_per_match", "avg_home_goals", "avg_away_goals"]
    ] = kpi[["avg_goals_per_match", "avg_home_goals", "avg_away_goals"]].round(2)
    return kpi.sort_values("season_start_year").reset_index(drop=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    matches = read_matches()
    league_table = build_league_table(matches)
    match_kpi = build_match_kpi(matches)
    league_table.to_csv(OUTPUT_DIR / "fct_league_table.csv", index=False)
    match_kpi.to_csv(OUTPUT_DIR / "fct_match_kpi.csv", index=False)
    print(f"Exported {len(league_table):,} league-table rows")
    print(f"Exported {len(match_kpi):,} match-KPI rows")


if __name__ == "__main__":
    main()
