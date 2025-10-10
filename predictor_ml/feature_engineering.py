# feature_engineering.py (Corrected for 'gamedate_home')
"""
Feature engineering for NFL Game Outcome Predictor
- Loads processed home/away dataset from data_builder.py
- Creates robust, non-leaky, point-in-time (season-to-date) features
- Saves an ML-ready dataset
"""

import pandas as pd
import numpy as np
import os

# ---------------- CONFIG ----------------
INPUT_FILE = "data/processed_games.parquet"
OUTPUT_FILE = "data/point_in_time_features.parquet"

# ---------------- UTILS ----------------
def ensure_dir(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)

# ---------------- LOAD DATA ----------------
print(f"Loading dataset: {INPUT_FILE}")
df = pd.read_parquet(INPUT_FILE)
# --- FIX: Use the correct column name 'gamedate_home' for sorting ---
df = df.sort_values(by='gamedate_home').reset_index(drop=True)
print(f"Initial shape: {df.shape}")

# ---------------- ENGINEER POINT-IN-TIME FEATURES ----------------
print("Engineering point-in-time (season-to-date) features...")

stats_to_average = [
    'points_for', 'points_against', 'pass_yds', 'rush_yds', 'turnovers'
]

# 1. Unpivot the data to a long format
# --- FIX: Use 'gamedate_home' as the canonical date for each game ---
home_stats = df[['game_id', 'season_home', 'gamedate_home', 'team_home'] + [f'{s}_home' for s in stats_to_average]].rename(
    columns={'team_home': 'team', 'season_home': 'season', 'gamedate_home': 'gamedate'}
)
away_stats = df[['game_id', 'season_away', 'gamedate_home', 'team_away'] + [f'{s}_away' for s in stats_to_average]].rename(
    columns={'team_away': 'team', 'season_away': 'season', 'gamedate_home': 'gamedate'}
)

home_stats.columns = [c.replace('_home', '') for c in home_stats.columns]
away_stats.columns = [c.replace('_away', '') for c in away_stats.columns]

long_df = pd.concat([home_stats, away_stats]).sort_values(by='gamedate').reset_index(drop=True)

# 2. Calculate the season-to-date expanding average for each stat
for stat in stats_to_average:
    long_df[f'season_avg_{stat}'] = long_df.groupby(['season', 'team'])[stat].transform(
        lambda x: x.shift(1).expanding().mean()
    )

# 3. Merge the new features back into the main DataFrame
df = pd.merge(
    df, long_df,
    left_on=['game_id', 'team_home'], right_on=['game_id', 'team'],
    suffixes=('', '_drop_h'), how='left'
).rename(columns={f'season_avg_{s}': f'home_season_avg_{s}' for s in stats_to_average})

df = pd.merge(
    df, long_df,
    left_on=['game_id', 'team_away'], right_on=['game_id', 'team'],
    suffixes=('', '_drop_a'), how='left'
).rename(columns={f'season_avg_{s}': f'away_season_avg_{s}' for s in stats_to_average})

# Clean up redundant columns
df = df.loc[:, ~df.columns.str.contains('_drop')]
df = df.drop(columns=[col for col in ['team', 'gamedate', 'points_for', 'points_against', 'pass_yds', 'rush_yds', 'turnovers'] if col in df.columns])

# 4. Create final differential features
print("Creating differential features...")
for stat in stats_to_average:
    df[f'season_avg_{stat}_diff'] = df[f'home_season_avg_{stat}'] - df[f'away_season_avg_{stat}']

# ---------------- SELECT FINAL FEATURES AND SAVE ----------------
final_features = [f'season_avg_{stat}_diff' for stat in stats_to_average]
target = 'label_home_win'

# Prepare the final dataset for saving
df_final = df[['game_id', 'season_home', 'week_home', 'gamedate_home', 'team_home', 'team_away', target] + final_features].rename(
    columns={'season_home': 'season', 'week_home': 'week', 'gamedate_home': 'gamedate'}
)
df_final = df_final.dropna().reset_index(drop=True)

# Optimize types
for col in df_final.columns:
    if df_final[col].dtype == 'float64':
        df_final[col] = pd.to_numeric(df_final[col], downcast='float')

print(f"Final shape after creating features and dropping NAs: {df_final.shape}")

# Save the ML-ready dataset
ensure_dir(OUTPUT_FILE)
df_final.to_parquet(OUTPUT_FILE, index=False)
print(f"✅ Saved ML-ready dataset with point-in-time features: {OUTPUT_FILE}")