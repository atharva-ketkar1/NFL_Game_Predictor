# verify.py (Final Version with Tolerance Adjustment)
import pandas as pd
import numpy as np
import os
import random
import sys

# --- CONFIGURATION ---
DATA_DIR = 'data'
MODEL_DATA_FILE = os.path.join(DATA_DIR, "model_ready_data.csv")
YEARS = range(2019, 2025)
NUM_SIMULATIONS = 500 # Number of random games to check

def run_single_check(model_df: pd.DataFrame, all_raw_data_df: pd.DataFrame) -> str | None:
    """
    Performs a single random check and returns an error string if a check fails, otherwise None.
    """
    try:
        game_to_check = model_df.sample(1).iloc[0]
        game_key = game_to_check['GameKey']
        season = game_to_check['season']
        home_team = game_to_check['home_team']
        away_team = game_to_check['away_team']

        if random.choice([True, False]):
            team_to_check, is_home_game = home_team, True
        else:
            team_to_check, is_home_game = away_team, False
        
        # Check 1: Raw Stat Validity
        raw_score_col = 'home_Points' if is_home_game else 'away_Points'
        final_score_col = 'home_score' if is_home_game else 'away_score'
        if game_to_check[raw_score_col] != game_to_check[final_score_col]:
             return f"GameKey {game_key}: FAILED Check 1 (Raw Stat)."

        # Check 2: Rolling Average Validity
        team_games_in_season = all_raw_data_df[
            ((all_raw_data_df['home_team'] == team_to_check) | (all_raw_data_df['away_team'] == team_to_check)) &
            (all_raw_data_df['season'] == season)
        ].sort_values('game_date')
        
        past_games = team_games_in_season[team_games_in_season['game_date'] < game_to_check['game_date']]
        
        if not past_games.empty:
            past_scores = np.where(past_games['home_team'] == team_to_check, past_games['home_Points'], past_games['away_Points'])
            window_size = 5
            manual_rolling_avg = round(past_scores[-window_size:].mean(), 4)
            
            rolling_avg_col = 'home_rolling_avg_Points' if is_home_game else 'away_rolling_avg_Points'
            rolling_avg_from_file = game_to_check[rolling_avg_col]

            if not np.isclose(manual_rolling_avg, rolling_avg_from_file):
                return f"GameKey {game_key}: FAILED Check 2 (Rolling Avg). Perspective: {team_to_check}. Manual: {manual_rolling_avg:.4f}, File: {rolling_avg_from_file}"
        
        # Check 3: Differential Feature Validity
        home_rolling = game_to_check['home_rolling_avg_Points']
        away_rolling = game_to_check['away_rolling_avg_Points']
        diff_from_file = game_to_check['rolling_avg_Points_diff']
        manual_diff = round(home_rolling - away_rolling, 4)
        
        # --- CHANGED: Added atol=1e-4 to account for minor rounding differences ---
        if not np.isclose(manual_diff, diff_from_file, atol=1e-4):
            return f"GameKey {game_key}: FAILED Check 3 (Differential). Manual: {manual_diff:.4f}, File: {diff_from_file}"

        return None

    except Exception as e:
        return f"An unexpected error occurred during check for GameKey {game_to_check.get('GameKey', 'N/A')}: {e}"

def main():
    """Main function to run the Monte Carlo simulation."""
    print(f"--- Starting Monte Carlo Verification ({NUM_SIMULATIONS} simulations) ---")
    
    try:
        model_df = pd.read_csv(MODEL_DATA_FILE)
        model_df['game_date'] = pd.to_datetime(model_df['game_date'])

        raw_files = [os.path.join(DATA_DIR, f"nfl_raw_data_{year}.csv") for year in YEARS]
        if not any(os.path.exists(f) for f in raw_files):
            print("❌ Error: No raw data files found in the 'data' directory.")
            return

        all_raw_data_df = pd.concat(
            [pd.read_csv(f) for f in raw_files if os.path.exists(f)],
            ignore_index=True
        )
        game_month = pd.to_numeric(all_raw_data_df['home_GameDateShort'].str.split('/').str[0], errors='coerce')
        calendar_year = np.where(game_month <= 2, all_raw_data_df['season'] + 1, all_raw_data_df['season'])
        all_raw_data_df['game_date'] = pd.to_datetime(
            all_raw_data_df['home_GameDateShort'] + '/' + pd.Series(calendar_year).astype(str),
            errors='coerce'
        )
        all_raw_data_df = all_raw_data_df.dropna(subset=['game_date']).sort_values(by='game_date')

    except FileNotFoundError:
        print(f"❌ Error: Could not find '{MODEL_DATA_FILE}'. Please run feature engineering.")
        return

    errors_found = []
    for i in range(NUM_SIMULATIONS):
        sys.stdout.write(f"\rRunning simulation {i + 1}/{NUM_SIMULATIONS}...")
        sys.stdout.flush()
        
        error = run_single_check(model_df, all_raw_data_df)
        if error:
            errors_found.append(error)
    
    print("\n--- Monte Carlo Simulation Complete ---")

    if not errors_found:
        print(f"\n✅ All {NUM_SIMULATIONS} random checks passed successfully!")
        print("Your data pipeline is generating accurate and valid features based on season-specific logic.")
    else:
        print(f"\n❌ Found {len(errors_found)} inconsistencies in {NUM_SIMULATIONS} checks:")
        for err in sorted(list(set(errors_found))):
            print(f"  - {err}")

if __name__ == '__main__':
    main()