import pandas as pd
import os

# --- Load Datasets ---
# Define the data directory
data_dir = 'data'

# Load the original game data
try:
    df_games = pd.read_csv(os.path.join(data_dir, 'model_ready_data.csv'))
    print("✅ Successfully loaded model_ready_data.csv")
except FileNotFoundError:
    print("❌ Error: 'model_ready_data.csv' not found. Please ensure it's in the 'data' directory.")
    exit()

# Load the betting data downloaded from Kaggle
try:
    df_odds = pd.read_csv(os.path.join(data_dir, 'spreadspoke_scores.csv'))
    print("✅ Successfully loaded spreadspoke_scores.csv")
except FileNotFoundError:
    print("❌ Error: 'spreadspoke_scores.csv' not found. Please download it and place it in the 'data' directory.")
    exit()

# --- Step 1: Merge Betting Data ---

# Standardize Team Names
team_name_map = {
    'Arizona Cardinals': 'ARI', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GNB',
    'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KCC', 'Los Angeles Chargers': 'LAC', 'Los Angeles Rams': 'LAR',
    'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN', 'New England Patriots': 'NEP',
    'New Orleans Saints': 'NOS', 'New York Giants': 'NYG', 'New York Jets': 'NYJ',
    'Las Vegas Raiders': 'LVR', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SFO', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TAM',
    'Tennessee Titans': 'TEN', 'Washington Football Team': 'WAS', 'Washington Commanders': 'WAS',
    'Washington Redskins': 'WAS', 'St. Louis Rams': 'LAR', 'San Diego Chargers': 'LAC',
    'Oakland Raiders': 'LVR'
}

df_odds['home_team_abbr'] = df_odds['team_home'].map(team_name_map)
df_odds['favorite_abbr'] = df_odds['team_favorite_id']

# Create the Unique Merge Key in Both DataFrames
df_games['merge_key'] = pd.to_datetime(df_games['game_date']).dt.strftime('%Y-%m-%d') + '_' + df_games['home_team']
df_odds['merge_key'] = pd.to_datetime(df_odds['schedule_date']).dt.strftime('%Y-%m-%d') + '_' + df_odds['home_team_abbr']

# Merge Datasets
df_merged = pd.merge(df_games, df_odds[['merge_key', 'favorite_abbr', 'spread_favorite']], on='merge_key', how='left')

# Create Home Team Point Spread Feature
def create_home_spread(row):
    if pd.isna(row['spread_favorite']):
        return None
    if row['home_team'] == row['favorite_abbr']:
        return row['spread_favorite']
    else:
        return -row['spread_favorite']

df_merged['home_team_spread'] = df_merged.apply(create_home_spread, axis=1)
print("✅ Betting data merged and 'home_team_spread' feature created.")

# --- Step 2: Add Final Contextual Features ---

# Create "Coming Off Bye Week" features
df_merged['home_coming_off_bye'] = (df_merged['home_days_of_rest'] > 10).astype(int)
df_merged['away_coming_off_bye'] = (df_merged['away_days_of_rest'] > 10).astype(int)
df_merged['coming_off_bye_diff'] = df_merged['home_coming_off_bye'] - df_merged['away_coming_off_bye']
print("✅ Contextual features for bye weeks created.")

# --- Finalize and Save ---
# Drop temporary merge columns
df_final = df_merged.drop(columns=['merge_key', 'favorite_abbr', 'spread_favorite'])

# Save the final feature-engineered dataset
output_filename = 'feature_engineered_data.csv'
df_final.to_csv(output_filename, index=False)
print(f"\n🚀 Success! Final dataset saved as '{output_filename}'")