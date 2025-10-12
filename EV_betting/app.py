# app.py

from flask import Flask, render_template
import pandas as pd
import os
import glob
import re
from collections import defaultdict

app = Flask(__name__)

# --- Normalization Maps ---
TEAM_MAP = {
    'arizona cardinals': 'Arizona Cardinals', 'ari cardinals': 'Arizona Cardinals', 'atlanta falcons': 'Atlanta Falcons', 'atl falcons': 'Atlanta Falcons', 'baltimore ravens': 'Baltimore Ravens', 'bal ravens': 'Baltimore Ravens', 'buffalo bills': 'Buffalo Bills', 'buf bills': 'Buffalo Bills', 'carolina panthers': 'Carolina Panthers', 'car panthers': 'Carolina Panthers', 'chicago bears': 'Chicago Bears', 'chi bears': 'Chicago Bears', 'cincinnati bengals': 'Cincinnati Bengals', 'cin bengals': 'Cincinnati Bengals', 'cleveland browns': 'Cleveland Browns', 'cle browns': 'Cleveland Browns', 'dallas cowboys': 'Dallas Cowboys', 'dal cowboys': 'Dallas Cowboys', 'denver broncos': 'Denver Broncos', 'den broncos': 'Denver Broncos', 'detroit lions': 'Detroit Lions', 'det lions': 'Detroit Lions', 'green bay packers': 'Green Bay Packers', 'gb packers': 'Green Bay Packers', 'houston texans': 'Houston Texans', 'hou texans': 'Houston Texans', 'indianapolis colts': 'Indianapolis Colts', 'ind colts': 'Indianapolis Colts', 'jacksonville jaguars': 'Jacksonville Jaguars', 'jac jaguars': 'Jacksonville Jaguars', 'jax jaguars': 'Jacksonville Jaguars', 'kansas city chiefs': 'Kansas City Chiefs', 'kc chiefs': 'Kansas City Chiefs', 'las vegas raiders': 'Las Vegas Raiders', 'lv raiders': 'Las Vegas Raiders', 'los angeles chargers': 'Los Angeles Chargers', 'la chargers': 'Los Angeles Chargers', 'los angeles rams': 'Los Angeles Rams', 'la rams': 'Los Angeles Rams', 'miami dolphins': 'Miami Dolphins', 'mia dolphins': 'Miami Dolphins', 'minnesota vikings': 'Minnesota Vikings', 'min vikings': 'Minnesota Vikings', 'new england patriots': 'New England Patriots', 'ne patriots': 'New England Patriots', 'new orleans saints': 'New Orleans Saints', 'no saints': 'New Orleans Saints', 'new york giants': 'New York Giants', 'ny giants': 'New York Giants', 'new york jets': 'New York Jets', 'ny jets': 'New York Jets', 'philadelphia eagles': 'Philadelphia Eagles', 'phi eagles': 'Philadelphia Eagles', 'pittsburgh steelers': 'Pittsburgh Steelers', 'pit steelers': 'Pittsburgh Steelers', 'san francisco 49ers': 'San Francisco 49ers', 'sf 49ers': 'San Francisco 49ers', 'seattle seahawks': 'Seattle Seahawks', 'sea seahawks': 'Seattle Seahawks', 'tampa bay buccaneers': 'Tampa Bay Buccaneers', 'tb buccaneers': 'Tampa Bay Buccaneers', 'tennessee titans': 'Tennessee Titans', 'ten titans': 'Tennessee Titans', 'washington commanders': 'Washington Commanders', 'was commanders': 'Washington Commanders', 'wsh commanders': 'Washington Commanders',
}
PROP_TYPE_MAP = {
    'Total Receptions': 'Receptions', 'Pass Completions': 'Pass Completions',
    'Receiving Yds': 'Receiving Yards', 'Rushing Yds': 'Rushing Yards',
    'Passing Yds': 'Passing Yards', 'Passing TDs': 'Passing Touchdowns', 'Pass Yards': 'Passing Yards',
    'Pass Attempts': 'Passing Attempts', 'Pass + Rushing Yds': 'Passing + Rushing Yards',
    '1st Qtr Passing Yds': '1st Quarter Passing Yards', '1st Qtr Receiving Yds': '1st Quarter Receiving Yards',
    'Longest Pass': 'Longest Pass Completion', 'Longest Reception': 'Longest Reception',
}

# --- Helper Functions ---
def normalize_game_name(game_name, team_map):
    if not isinstance(game_name, str) or ' @ ' not in game_name: return game_name
    away_str, home_str = [s.strip() for s in game_name.split(' @ ')]
    normalized_away = team_map.get(away_str.lower(), away_str)
    normalized_home = team_map.get(home_str.lower(), home_str)
    return f"{normalized_away} @ {normalized_home}"

def normalize_prop_type(prop_type, prop_map): return prop_map.get(prop_type, prop_type)
def normalize_player_name(name):
    if not isinstance(name, str): return ''
    return re.sub(r'[^a-z0-9\s]', '', name.lower()).strip()
def convert_odds_to_prob(odds):
    try:
        odds = float(odds)
        if odds > 0: return 100 / (odds + 100)
        else: return abs(odds) / (abs(odds) + 100)
    except (ValueError, TypeError): return 0

# --- Core Logic Functions ---
def find_arbitrage_opportunities(props_df):
    # ... (This function remains unchanged) ...
    if props_df.empty: return []
    df = props_df.copy()
    df['over_odds'] = pd.to_numeric(df['over_odds'], errors='coerce')
    df['under_odds'] = pd.to_numeric(df['under_odds'], errors='coerce')
    df.dropna(subset=['over_odds', 'under_odds', 'line', 'player_name_norm'], inplace=True)
    opportunities = []
    grouped = df.groupby(['player_name_norm', 'prop_type_norm', 'line'])
    for _, group in grouped:
        if len(group['sportsbook'].unique()) < 2: continue
        best_over_row = group.loc[group['over_odds'].idxmax()]
        best_under_row = group.loc[group['under_odds'].idxmax()]
        prob_over = convert_odds_to_prob(best_over_row['over_odds'])
        prob_under = convert_odds_to_prob(best_under_row['under_odds'])
        if (prob_over + prob_under) < 1.0:
            profit_margin = (1 - (prob_over + prob_under)) * 100
            opportunities.append({
                'player_name': group['player_name'].iloc[0], 'prop_type': group['prop_type_norm'].iloc[0],
                'line': group['line'].iloc[0],
                'bet_on_over': {'sportsbook': best_over_row['sportsbook'], 'odds': int(best_over_row['over_odds'])},
                'bet_on_under': {'sportsbook': best_under_row['sportsbook'], 'odds': int(best_under_row['under_odds'])},
                'profit_margin': f"{profit_margin:.2f}%"
            })
    return opportunities


def get_combined_data():
    data_dir = 'nfl_data'
    if not os.path.exists(data_dir): return None, None, "Data folder 'nfl_data' not found.", None, []
    
    sources = ['fanduel', 'draftkings']
    latest_files = {f'{s}_{t}': max(glob.glob(os.path.join(data_dir, f'{s}_*_week_*_{t}.csv')), key=os.path.getctime) 
                    for s in sources for t in ['props', 'game_lines'] if glob.glob(os.path.join(data_dir, f'{s}_*_week_*_{t}.csv'))}
    
    if not latest_files: return None, None, "No data files found.", None, []

    all_props_dfs = []
    for key, path in latest_files.items():
        if 'props' in key:
            df = pd.read_csv(path)
            if 'player' in df.columns and 'player_name' not in df.columns:
                df.rename(columns={'player': 'player_name'}, inplace=True)
            all_props_dfs.append(df)

    if not all_props_dfs: return None, None, "Could not load player prop data.", None, []
    
    props_df = pd.concat(all_props_dfs, ignore_index=True)
    props_df.drop_duplicates(inplace=True)
    props_df.dropna(subset=['game', 'player_name', 'over_odds', 'under_odds'], inplace=True)
    props_df.fillna('', inplace=True)
    props_df['game_norm'] = props_df['game'].astype(str).apply(lambda g: normalize_game_name(g, TEAM_MAP))
    props_df['player_name_norm'] = props_df['player_name'].apply(normalize_player_name)
    props_df['prop_type_norm'] = props_df['prop_type'].apply(lambda p: normalize_prop_type(p, PROP_TYPE_MAP))

    # --- Data Enrichment (Team Info) ---
    player_team_map = {}
    fd_data = props_df[(props_df['sportsbook'] == 'FanDuel') & (props_df['team_name'] != '')].copy()
    if not fd_data.empty:
        fd_unique_players = fd_data.drop_duplicates(subset=['player_name_norm'], keep='first')
        player_team_map = fd_unique_players.set_index('player_name_norm')[['team_name', 'team_logo']].to_dict('index')
    
    def get_team_info(row, key): return player_team_map.get(row['player_name_norm'], {}).get(key, '')
    
    missing_team_mask = props_df['team_name'] == ''
    if missing_team_mask.any():
        props_df.loc[missing_team_mask, 'team_name'] = props_df[missing_team_mask].apply(get_team_info, key='team_name', axis=1)
        props_df.loc[missing_team_mask, 'team_logo'] = props_df[missing_team_mask].apply(get_team_info, key='team_logo', axis=1)
    
    # --- Process Game Lines ---
    lines_df = pd.DataFrame()
    if 'fanduel_game_lines' in latest_files:
        df = pd.read_csv(latest_files['fanduel_game_lines'])
        df.dropna(subset=['game'], inplace=True)
        df['game_norm'] = df['game'].astype(str).apply(lambda g: normalize_game_name(g, TEAM_MAP))
        lines_df = df.drop_duplicates(subset=['game_norm'], keep='first').set_index('game_norm')

    week_number = 'N/A'
    try: week_number = os.path.basename(list(latest_files.values())[0]).split('_week_')[1].split('_')[0]
    except (IndexError, AttributeError): pass
    
    # --- MODIFICATION START: Re-structured output for the new template ---
    sportsbooks = sorted(props_df['sportsbook'].unique().tolist())
    output_structure = defaultdict(lambda: {'game_lines': None, 'teams': {}})
    
    # Group data for easier iteration in the template
    grouped = props_df.groupby(['game_norm', 'team_name', 'player_name', 'prop_type_norm'])
    
    for (game, team, player, prop_type), group in grouped:
        if not all([game, team, player]): continue

        # Initialize dictionaries if they don't exist
        if team not in output_structure[game]['teams']:
            output_structure[game]['teams'][team] = {'logo': group['team_logo'].iloc[0], 'players': {}}
        if player not in output_structure[game]['teams'][team]['players']:
            output_structure[game]['teams'][team]['players'][player] = {'props': {}}

        # For each prop type, create a list of lines with odds from all sportsbooks
        lines = []
        for line, line_group in group.groupby('line'):
            odds_by_book = {book: {'over': '—', 'under': '—'} for book in sportsbooks}
            for _, row in line_group.iterrows():
                odds_by_book[row['sportsbook']] = {'over': row['over_odds'], 'under': row['under_odds']}
            lines.append({'line': line, 'odds': odds_by_book})
        
        output_structure[game]['teams'][team]['players'][player]['props'][prop_type] = sorted(lines, key=lambda x: x['line'])

    for game in output_structure:
        if game in lines_df.index:
            output_structure[game]['game_lines'] = lines_df.loc[game].to_dict()
    # --- MODIFICATION END ---
            
    return props_df, output_structure, None, week_number, sportsbooks

# --- Flask Routes ---
@app.route('/')
def index():
    raw_props_df, final_data, error_msg, week_number, sportsbooks = get_combined_data()
    arbitrage_ops = find_arbitrage_opportunities(raw_props_df) if raw_props_df is not None else []
    
    return render_template('index.html', final_data=final_data, error_msg=error_msg,
                           week_number=week_number, arbitrage_ops=arbitrage_ops,
                           sportsbooks=sportsbooks) # Pass sportsbooks to the template

if __name__ == '__main__':
    app.run(debug=True)