# feature_engineering.py (With Days of Rest Feature)
import pandas as pd
import numpy as np
import os

# --- CONFIGURATION ---
DATA_DIR = 'data'
YEARS = range(2019, 2025)
ROLLING_WINDOW_SIZE = 5 # Number of past games to average over
STANDARD_REST_DAYS = 7 # Assume 7 days of rest for the first game of the season

def main():
    """Main function to load, process, and save the data."""
    print("Starting feature engineering process...")

    # 1. Load and Combine All Raw Data
    all_raw_df = pd.concat(
        [pd.read_csv(os.path.join(DATA_DIR, f'nfl_raw_data_{year}.csv')) for year in YEARS if os.path.exists(os.path.join(DATA_DIR, f'nfl_raw_data_{year}.csv'))],
        ignore_index=True
    )
    
    game_month = pd.to_numeric(all_raw_df['home_GameDateShort'].str.split('/').str[0], errors='coerce')
    calendar_year = np.where(game_month <= 2, all_raw_df['season'] + 1, all_raw_df['season'])
    all_raw_df['game_date'] = pd.to_datetime(
        all_raw_df['home_GameDateShort'] + '/' + pd.Series(calendar_year).astype(str),
        errors='coerce'
    )
    all_raw_df = all_raw_df.dropna(subset=['game_date']).sort_values(by='game_date')
    print(f"✓ Loaded and combined data for {len(all_raw_df)} games from {YEARS[0]}-{YEARS[-1]}.")

    # 2. Restructure the data into a long format
    home_stats = all_raw_df[[col for col in all_raw_df.columns if col.startswith('home_') or col in ['GameKey', 'game_date', 'season']]]
    away_stats = all_raw_df[[col for col in all_raw_df.columns if col.startswith('away_') or col in ['GameKey', 'game_date', 'season']]]

    home_stats = home_stats.rename(columns=lambda col: col.replace('home_', ''))
    away_stats = away_stats.rename(columns=lambda col: col.replace('away_', ''))
    
    team_game_df = pd.concat([home_stats, away_stats], ignore_index=True).sort_values(by=['team', 'game_date'])
    print("✓ Restructured data to team-game format.")
    
    # 3. Calculate rolling averages on the correctly structured data
    stats_to_average = [
        'Points', 'TotalNetYards', 'NetYardsPassing', 'NetYardsRushing', 'FumblesLost', 'Interceptions'
    ]
    team_game_df['Turnovers'] = team_game_df['FumblesLost'] + team_game_df['Interceptions']
    stats_to_average.append('Turnovers')

    for stat in stats_to_average:
        team_game_df[f'rolling_avg_{stat}'] = team_game_df.groupby(['team', 'season'])[stat].transform(
            lambda x: x.shift(1).rolling(window=ROLLING_WINDOW_SIZE, min_periods=1).mean()
        )
    print(f"✓ Calculated {ROLLING_WINDOW_SIZE}-game SEASON-SPECIFIC rolling averages.")

    # --- NEW: CALCULATE DAYS OF REST ---
    print("Calculating days of rest for each team...")
    # Get the date of the previous game for each team within each season
    team_game_df['previous_game_date'] = team_game_df.groupby(['team', 'season'])['game_date'].shift(1)
    
    # Calculate the difference in days
    team_game_df['days_of_rest'] = (team_game_df['game_date'] - team_game_df['previous_game_date']).dt.days
    
    # Fill missing values (for the first game of the season) with a standard number of rest days
    team_game_df['days_of_rest'] = team_game_df['days_of_rest'].fillna(STANDARD_REST_DAYS)
    print("✓ Days of rest feature created.")
    # --- END OF NEW SECTION ---

    # 4. Merge rolling stats and new features back into the original game-by-game format
    home_rolling_stats = team_game_df.rename(columns={'team': 'home_team'})
    away_rolling_stats = team_game_df.rename(columns={'team': 'away_team'})
    
    rolling_cols = [f'rolling_avg_{stat}' for stat in stats_to_average]
    new_feature_cols = ['days_of_rest'] # Add our new feature here
    
    final_df = pd.merge(
        all_raw_df, 
        home_rolling_stats[['GameKey', 'home_team', 'season'] + rolling_cols + new_feature_cols], 
        on=['GameKey', 'home_team', 'season'], 
        how='left'
    )
    final_df = final_df.rename(columns={col: f'home_{col}' for col in rolling_cols + new_feature_cols})
    
    final_df = pd.merge(
        final_df, 
        away_rolling_stats[['GameKey', 'away_team', 'season'] + rolling_cols + new_feature_cols], 
        on=['GameKey', 'away_team', 'season'], 
        how='left'
    )
    final_df = final_df.rename(columns={col: f'away_{col}' for col in rolling_cols + new_feature_cols})

    # 5. Create differential features for all relevant stats
    for stat in stats_to_average:
        final_df[f'rolling_avg_{stat}_diff'] = final_df[f'home_rolling_avg_{stat}'] - final_df[f'away_rolling_avg_{stat}']
    
    final_df['days_of_rest_diff'] = final_df['home_days_of_rest'] - final_df['away_days_of_rest'] # Create the differential for rest days
    print("✓ Created matchup differential features.")
    
    # 6. Final cleanup and reordering
    key_columns = ['home_rolling_avg_Points', 'away_rolling_avg_Points', 'home_days_of_rest']
    final_df = final_df.dropna(subset=key_columns).reset_index(drop=True)
    
    # Reorder columns to keep it clean (optional but good practice)
    id_cols = ['GameKey', 'season', 'game_date', 'home_team', 'away_team']
    target_cols = ['home_score', 'away_score', 'home_team_win']
    engineered_cols = sorted([col for col in final_df.columns if 'rolling_avg' in col or 'days_of_rest' in col])
    
    # Combine all columns, ensuring no duplicates, with engineered features grouped together
    all_cols_ordered = id_cols + target_cols + engineered_cols
    remaining_cols = sorted([col for col in final_df.columns if col not in all_cols_ordered])
    final_df = final_df[all_cols_ordered + remaining_cols]
    
    final_df = final_df.round(4)
    print("✓ Final cleanup and rounding complete.")
    
    output_path = os.path.join(DATA_DIR, 'model_ready_data.csv')
    final_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Feature engineering complete!")
    print(f"Model-ready data with 'Days of Rest' feature saved to: {output_path}")

if __name__ == '__main__':
    main()