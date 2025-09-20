# feature_engineering.py (Corrected for Season-Specific Rolling Averages)
import pandas as pd
import numpy as np
import os

# --- CONFIGURATION ---
DATA_DIR = 'data'
YEARS = range(2019, 2025)
ROLLING_WINDOW_SIZE = 5 # Number of past games to average over

def main():
    """Main function to load, process, and save the data."""
    print("Starting feature engineering process...")

    # 1. Load and Combine All Raw Data
    all_raw_df = pd.concat(
        [pd.read_csv(os.path.join(DATA_DIR, f'nfl_raw_data_{year}.csv')) for year in YEARS],
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

    # 2. Restructure the data into a long format (one row per team per game)
    # --- CHANGED: Added 'season' to the list of columns to keep ---
    home_stats = all_raw_df[[col for col in all_raw_df.columns if col.startswith('home_') or col in ['GameKey', 'game_date', 'season']]]
    away_stats = all_raw_df[[col for col in all_raw_df.columns if col.startswith('away_') or col in ['GameKey', 'game_date', 'season']]]

    home_stats = home_stats.rename(columns=lambda col: col.replace('home_', ''))
    away_stats = away_stats.rename(columns=lambda col: col.replace('away_', ''))
    
    # Season is now correctly included in this DataFrame
    team_game_df = pd.concat([home_stats, away_stats], ignore_index=True).sort_values(by=['team', 'game_date'])
    print("✓ Restructured data to team-game format for accurate calculations.")
    
    # 3. Calculate rolling averages on the correctly structured data
    stats_to_average = [
        'Points', 'TotalNetYards', 'NetYardsPassing', 'NetYardsRushing', 'FumblesLost', 'Interceptions'
    ]
    team_game_df['Turnovers'] = team_game_df['FumblesLost'] + team_game_df['Interceptions']
    stats_to_average.append('Turnovers')

    for stat in stats_to_average:
        # --- CHANGED: Group by both 'team' AND 'season' ---
        # This resets the rolling calculation for each team at the start of a new season.
        team_game_df[f'rolling_avg_{stat}'] = team_game_df.groupby(['team', 'season'])[stat].transform(
            lambda x: x.shift(1).rolling(window=ROLLING_WINDOW_SIZE, min_periods=1).mean()
        )
    print(f"✓ Calculated {ROLLING_WINDOW_SIZE}-game SEASON-SPECIFIC rolling averages for all teams.")

    # 4. Merge the rolling stats back into the original game-by-game format
    home_rolling_stats = team_game_df.rename(columns={'team': 'home_team'})
    away_rolling_stats = team_game_df.rename(columns={'team': 'away_team'})
    
    rolling_cols = [f'rolling_avg_{stat}' for stat in stats_to_average]
    
    # The merge keys now need to include 'season' to ensure the correct stats are matched
    final_df = pd.merge(
        all_raw_df, 
        home_rolling_stats[['GameKey', 'home_team', 'season'] + rolling_cols], 
        on=['GameKey', 'home_team', 'season'], 
        how='left'
    )
    final_df = final_df.rename(columns={col: f'home_{col}' for col in rolling_cols})
    
    final_df = pd.merge(
        final_df, 
        away_rolling_stats[['GameKey', 'away_team', 'season'] + rolling_cols], 
        on=['GameKey', 'away_team', 'season'], 
        how='left'
    )
    final_df = final_df.rename(columns={col: f'away_{col}' for col in rolling_cols})

    # 5. Create differential features
    for stat in stats_to_average:
        final_df[f'rolling_avg_{stat}_diff'] = final_df[f'home_rolling_avg_{stat}'] - final_df[f'away_rolling_avg_{stat}']
    print("✓ Created matchup differential features.")
    
    # 6. Final cleanup and reordering
    key_rolling_columns = [
        'home_rolling_avg_Points',
        'away_rolling_avg_Points'
    ]
    # For week 1 games, these values will be NaN, so we should drop them as they can't be used for modeling.
    final_df = final_df.dropna(subset=key_rolling_columns).reset_index(drop=True)
    
    id_cols = ['GameKey', 'season', 'game_date', 'home_team', 'away_team']
    target_cols = ['home_score', 'away_score', 'home_team_win']
    rolling_cols = sorted([col for col in final_df.columns if 'rolling_avg' in col])
    existing_cols = id_cols + target_cols + rolling_cols
    remaining_cols = sorted([col for col in final_df.columns if col not in existing_cols])
    final_df = final_df[id_cols + target_cols + rolling_cols + remaining_cols]
    
    final_df = final_df.round(4)
    print("✓ Rounded all numerical columns to 4 decimal places.")
    
    output_path = os.path.join(DATA_DIR, 'model_ready_data.csv')
    final_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Feature engineering complete!")
    print(f"Model-ready data saved to: {output_path}")
    print(f"Original games: {len(all_raw_df)}, Final games with full features: {len(final_df)}")

if __name__ == '__main__':
    main()