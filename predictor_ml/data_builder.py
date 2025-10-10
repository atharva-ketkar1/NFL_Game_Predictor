# data_builder.py
"""
Data builder for Autonomous NFL Game Outcome Predictor.
- Uses nfl_data_py to fetch schedules + weekly player stats.
- Builds one row per game (home vs away), with rolling features.
- Outputs: data/processed_games.parquet
"""
import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

try:
    import nfl_data_py as nd
except Exception as e:
    raise ImportError("Please install nfl_data_py: pip install nfl-data-py") from e

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def fetch_schedules(seasons):
    print(f"Fetching schedules for {seasons}")
    sched = nd.import_schedules(seasons)
    sched["gamedate"] = pd.to_datetime(sched["gameday"])
    # keep relevant columns
    sched = sched[
        [
            "game_id", "season", "week", "gamedate",
            "home_team", "away_team", "home_score", "away_score"
        ]
    ]
    return sched

def fetch_weekly_stats(seasons):
    print(f"Fetching weekly stats for {seasons}")
    weekly = nd.import_weekly_data(seasons)
    if "team" not in weekly.columns and "recent_team" in weekly.columns:
        weekly = weekly.rename(columns={"recent_team": "team"})
    return weekly

def attach_game_id_and_home(weekly, sched):
    # merge for home team
    home_df = weekly.merge(
        sched,
        left_on=["season", "week", "team"],
        right_on=["season", "week", "home_team"],
        how="inner"
    )
    home_df["is_home"] = True

    # merge for away team
    away_df = weekly.merge(
        sched,
        left_on=["season", "week", "team"],
        right_on=["season", "week", "away_team"],
        how="inner"
    )
    away_df["is_home"] = False

    # combine
    combined = pd.concat([home_df, away_df], ignore_index=True)
    return combined

def compute_rolling_features(df, lookbacks=(3, 5, 10)):
    df = df.sort_values(["team", "gamedate"]).copy()
    for L in lookbacks:
        df[f"rolling_pf_{L}"] = (
            df.groupby("team")["points_for"]
            .rolling(window=L, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
            .shift(1)
        )
        df[f"rolling_pa_{L}"] = (
            df.groupby("team")["points_against"]
            .rolling(window=L, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
            .shift(1)
        )
        df[f"rolling_net_{L}"] = df[f"rolling_pf_{L}"] - df[f"rolling_pa_{L}"]
    return df

def build_game_level_dataset(seasons, out_dir="data"):
    ensure_dir(out_dir)

    # fetch data
    sched = fetch_schedules(seasons)
    weekly = fetch_weekly_stats(seasons)

    # attach game_id and home/away flag
    weekly_team = attach_game_id_and_home(weekly, sched)

    # aggregate weekly player stats to team-game level
    agg = weekly_team.groupby([
        "season", "week", "team", "game_id", "is_home",
        "home_score", "away_score", "gamedate"
    ]).agg(
        pass_yds=("passing_yards", "sum"),
        rush_yds=("rushing_yards", "sum"),
        turnovers=("interceptions", "sum"),
        fantasy_pts=("fantasy_points", "sum")
    ).reset_index()

    # compute points for/against
    agg["points_for"] = np.where(agg["is_home"], agg["home_score"], agg["away_score"])
    agg["points_against"] = np.where(agg["is_home"], agg["away_score"], agg["home_score"])

    # compute rolling features
    agg = compute_rolling_features(agg, lookbacks=(3, 5, 10))

    # split home and away
    home = agg[agg["is_home"]].copy()
    away = agg[~agg["is_home"]].copy()

    # merge home vs away
    merged = home.merge(
        away,
        on="game_id",
        suffixes=("_home", "_away")
    )

    # create label
    merged["label_home_win"] = (merged["points_for_home"] > merged["points_for_away"]).astype(int)

    # save
    out_path = os.path.join(out_dir, "processed_games.parquet")
    merged.to_parquet(out_path, index=False)
    print(f"Saved dataset: {out_path}, Rows={len(merged)}")

    return out_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int, default=2015)
    parser.add_argument("--end-season", type=int, default=datetime.now().year - 1)
    parser.add_argument("--out-dir", type=str, default="data")
    args = parser.parse_args()

    seasons = list(range(args.start_season, args.end_season + 1))
    build_game_level_dataset(seasons, out_dir=args.out_dir)
