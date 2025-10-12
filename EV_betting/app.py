import os
import pandas as pd
import unicodedata
from collections import defaultdict
from flask import Flask, render_template

app = Flask(__name__)

# --- Constants & Mappings ---
TEAM_MAP = {
    'ARI Cardinals': 'Arizona Cardinals', 'ATL Falcons': 'Atlanta Falcons', 'BAL Ravens': 'Baltimore Ravens', 'BUF Bills': 'Buffalo Bills', 'CAR Panthers': 'Carolina Panthers', 'CHI Bears': 'Chicago Bears', 'CIN Bengals': 'Cincinnati Bengals', 'CLE Browns': 'Cleveland Browns', 'DAL Cowboys': 'Dallas Cowboys', 'DEN Broncos': 'Denver Broncos', 'DET Lions': 'Detroit Lions', 'GB Packers': 'Green Bay Packers', 'HOU Texans': 'Houston Texans', 'IND Colts': 'Indianapolis Colts', 'JAX Jaguars': 'Jacksonville Jaguars', 'KC Chiefs': 'Kansas City Chiefs', 'LV Raiders': 'Las Vegas Raiders', 'LA Chargers': 'Los Angeles Chargers', 'LA Rams': 'Los Angeles Rams', 'MIA Dolphins': 'Miami Dolphins', 'MIN Vikings': 'Minnesota Vikings', 'NE Patriots': 'New England Patriots', 'NO Saints': 'New Orleans Saints', 'NY Giants': 'New York Giants', 'NY Jets': 'New York Jets', 'PHI Eagles': 'Philadelphia Eagles', 'PIT Steelers': 'Pittsburgh Steelers', 'SF 49ers': 'San Francisco 49ers', 'SEA Seahawks': 'Seattle Seahawks', 'TB Buccaneers': 'Tampa Bay Buccaneers', 'TEN Titans': 'Tennessee Titans', 'WAS Commanders': 'Washington Commanders'
}

# --- Helper Functions ---
def normalize_player_name(name):
    return unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode("utf-8").lower().replace(" jr.", "").replace(" sr.", "").replace(".", "").replace("'", "")

def normalize_game_name(game_str, team_map):
    for short, long in team_map.items():
        game_str = game_str.replace(short, long)
    teams = game_str.split(' @ ')
    return ' @ '.join(sorted(teams))

def convert_odds_to_prob(odds):
    odds = float(odds)
    if odds > 0: return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)

def find_arbitrage_opportunities(props_df):
    if props_df is None or props_df.empty: return []
    df = props_df.copy()
    df.dropna(subset=['over_odds', 'under_odds', 'line', 'player_name_norm', 'prop_main', 'prop_qualifier'], inplace=True)
    opportunities = []
    grouped = df.groupby(['player_name_norm', 'prop_main', 'prop_qualifier', 'line'])
    for _, group in grouped:
        if len(group['sportsbook'].unique()) < 2: continue
        best_over_row = group.loc[group['over_odds'].idxmax()]
        best_under_row = group.loc[group['under_odds'].idxmax()]
        prob_over = convert_odds_to_prob(best_over_row['over_odds'])
        prob_under = convert_odds_to_prob(best_under_row['under_odds'])
        if (prob_over + prob_under) < 1.0:
            profit_margin = (1 - (prob_over + prob_under)) * 100
            prop_main = group['prop_main'].iloc[0]
            prop_qualifier = group['prop_qualifier'].iloc[0]
            prop_type_display = f"{prop_main} ({prop_qualifier})" if prop_qualifier and prop_qualifier != 'Full Game' else prop_main
            opportunities.append({
                'player_name': group['player_name'].iloc[0], 'prop_type': prop_type_display,
                'line': group['line'].iloc[0],
                'bet_on_over': {'sportsbook': best_over_row['sportsbook'], 'odds': int(best_over_row['over_odds'])},
                'bet_on_under': {'sportsbook': best_under_row['sportsbook'], 'odds': int(best_under_row['under_odds'])},
                'profit_margin': f"{profit_margin:.2f}%"
            })
    return opportunities

# --- MODIFICATION: Final, stable version of the prop parser ---
# --- MODIFICATION: Final, stable version of the prop parser ---
def parse_prop_type(prop_string: str) -> dict:
    prop_string = str(prop_string).strip()
    if not prop_string:
        return {'main': 'Unknown Prop', 'qualifier': ''}

    PROP_TYPE_MAP = {
        # Passing Touchdowns
        'Passing Touchdowns': 'Passing Touchdowns', 'Passing TDs': 'Passing Touchdowns', 'Pass TDs': 'Passing Touchdowns', 'TDs': 'Passing Touchdowns',
        
        # Passing Yards
        'Passing Yards': 'Passing Yards', 'Passing Yds': 'Passing Yards', 'Pass Yds': 'Passing Yards', 'Yds': 'Passing Yards',

        # Passing Completions
        'Passing Completions': 'Passing Completions', 'Completions': 'Passing Completions', 'Pass Completions': 'Passing Completions', 'Passing': 'Passing Completions',
        
        # Passing Attempts
        'Passing Attempts': 'Passing Attempts', 'Pass Attempts': 'Passing Attempts',
        
        # Interceptions
        'Interceptions Thrown': 'Interceptions Thrown', 'Interceptions': 'Interceptions Thrown', 'Interception': 'Interceptions Thrown',
        
        # Receiving Yards
        'Receiving Yards': 'Receiving Yards', 'Receiving Yds': 'Receiving Yards', 'Rec Yards': 'Receiving Yards',

        # Receptions
        'Receptions': 'Receptions', 'Total Receptions': 'Receptions', 'Reception': 'Receptions',
        
        # Rushing Yards
        'Rushing Yards': 'Rushing Yards', 'Rushing Yds': 'Rushing Yards', 'Rush Yards': 'Rushing Yards',

        # Rushing Attempts
        'Rushing Attempts': 'Rushing Attempts', 'Rush Attempts': 'Rushing Attempts',
        
        # Combo Props
        'Rushing + Receiving Yards': 'Rushing + Receiving Yards', 'Rushing + Receiving Yds': 'Rushing + Receiving Yards', 'Rush + Rec Yards': 'Rushing + Receiving Yards',
        'Passing + Rushing Yards': 'Passing + Rushing Yards', 'Passing + Rushing Yds': 'Passing + Rushing Yards',

        # Kicking Props
        'Field Goals Made': 'Field Goals Made', 'FG Made': 'Field Goals Made',
        'Kicking Points': 'Kicking Points', 'Kicking Pts': 'Kicking Points',
        'Extra Points Made': 'Extra Points Made', 'PAT Made': 'Extra Points Made',

        # Fantasy Points
        'Fantasy Points': 'Fantasy Points', 'WR/TE Fantasy Points': 'Fantasy Points', 'RB Fantasy Points': 'Fantasy Points', 'QB Fantasy Points': 'Fantasy Points',
    }
    
    qualifiers = {
        ' - 1st Half': '1st Half', ' - 1H': '1st Half',
        ' - 1st Quarter': '1st Quarter', ' - 1Q': '1st Quarter',
        '1st Qtr': '1st Quarter', 'Longest': 'Longest'
    }
    
    prop_qualifier = 'Full Game'
    main_prop_str = prop_string

    if 'Longest' in main_prop_str:
        prop_qualifier = 'Longest'
        main_prop_str = main_prop_str.replace('Longest', '').strip()
    else:
        for key, val in qualifiers.items():
            if key in main_prop_str:
                prop_qualifier = val
                main_prop_str = main_prop_str.replace(key, '').strip()
                break
            
    main_prop_str = main_prop_str.replace(' O/U', '').strip()
    main_prop = PROP_TYPE_MAP.get(main_prop_str, main_prop_str)

    if prop_qualifier == 'Longest':
        if main_prop in ['Receiving Yards', 'Receptions']: main_prop = 'Longest Reception'
        elif main_prop in ['Rushing Yards', 'Rush']: main_prop = 'Longest Rush'
        elif main_prop in ['Passing Completions', 'Pass']: main_prop = 'Longest Completion'
        prop_qualifier = '' # The qualifier is now part of the main name
        
    return {'main': main_prop, 'qualifier': prop_qualifier}


def extract_player_name(text, known_players):
    text = str(text)
    best_match = ''
    for player in sorted(known_players, key=len, reverse=True):
        if text.lower().startswith(player.lower()):
            best_match = player
            break
    return best_match if best_match else text

def get_combined_data():
    output_structure = defaultdict(lambda: {'game_lines': None, 'teams': {}})
    week_number = 'N/A'; sportsbooks = []
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    
    prop_files = { 'fanduel': 'fanduel_nfl_week_6_props.csv', 'draftkings': 'draftkings_nfl_week_6_props.csv' }
    
    if not os.path.isdir(data_dir):
        return None, output_structure, f"Error: Data directory not found at '{os.path.abspath(data_dir)}'.", week_number, sportsbooks

    fanduel_df = pd.read_csv(os.path.join(data_dir, prop_files['fanduel'])) if os.path.exists(os.path.join(data_dir, prop_files['fanduel'])) else pd.DataFrame()
    draftkings_df = pd.read_csv(os.path.join(data_dir, prop_files['draftkings'])) if os.path.exists(os.path.join(data_dir, prop_files['draftkings'])) else pd.DataFrame()
    
    if fanduel_df.empty and draftkings_df.empty:
        return None, output_structure, f"No prop data files found in '{os.path.abspath(data_dir)}'.", week_number, sportsbooks

    if not fanduel_df.empty:
        fanduel_df['sportsbook'] = 'Fanduel'

    if not draftkings_df.empty:
        draftkings_df.rename(columns={'player': 'player_name'}, inplace=True)
        draftkings_df['sportsbook'] = 'Draftkings'
        
        if not fanduel_df.empty:
            known_players = set(fanduel_df['player_name'].dropna().unique())
            
            def clean_dk_row(row):
                if row['player_name'] not in known_players:
                    full_prop_string = str(row['player_name'])
                    row['player_name'] = extract_player_name(full_prop_string, known_players)
                    row['prop_type'] = full_prop_string
                return row
                
            draftkings_df = draftkings_df.apply(clean_dk_row, axis=1)

    props_df = pd.concat([fanduel_df, draftkings_df], ignore_index=True)
    
    props_df.rename(columns={'over': 'over_odds', 'under': 'under_odds'}, inplace=True)

    for col in ['over_odds', 'under_odds']:
        if col in props_df.columns:
            props_df[col] = pd.to_numeric(props_df[col].astype(str).str.replace('−', '-'), errors='coerce')

    props_df.dropna(subset=['game', 'player_name', 'over_odds', 'under_odds', 'prop_type'], inplace=True)

    def get_prop_details(row):
        prop_string = str(row['prop_type'])
        player_name = str(row['player_name'])
        prop_only_string = prop_string
        if player_name and prop_string.lower().startswith(player_name.lower()):
            prop_only_string = prop_string[len(player_name):].strip()
        return parse_prop_type(prop_only_string)

    prop_details = props_df.apply(get_prop_details, axis=1)
    
    props_df['prop_main'] = prop_details.apply(lambda x: x['main'])
    props_df['prop_qualifier'] = prop_details.apply(lambda x: x['qualifier'])
    
    props_df['game_norm'] = props_df['game'].astype(str).apply(lambda g: normalize_game_name(g, TEAM_MAP))
    props_df['player_name_norm'] = props_df['player_name'].apply(normalize_player_name)
    
    for col in ['team_name', 'team_logo']:
        if col not in props_df.columns: props_df[col] = ''
    props_df.fillna({'team_name': '', 'team_logo': ''}, inplace=True)
    
    player_team_map = {}
    has_team_info_df = props_df[props_df['team_name'] != ''].copy()
    if not has_team_info_df.empty:
        unique_players = has_team_info_df.drop_duplicates(subset=['player_name_norm'], keep='last')
        player_team_map = unique_players.set_index('player_name_norm')[['team_name', 'team_logo']].to_dict('index')
    
    def get_team_info(row, key): return player_team_map.get(row['player_name_norm'], {}).get(key, '')
    
    missing_team_mask = props_df['team_name'] == ''
    if missing_team_mask.any():
        props_df.loc[missing_team_mask, 'team_name'] = props_df[missing_team_mask].apply(get_team_info, key='team_name', axis=1)
        props_df.loc[missing_team_mask, 'team_logo'] = props_df[missing_team_mask].apply(get_team_info, key='logo', axis=1)

    sportsbooks = sorted(props_df['sportsbook'].unique())
    props_df['grouping_team'] = props_df['team_name'].replace('', 'Unknown')
    
    grouped = props_df.groupby(['game_norm', 'grouping_team', 'player_name', 'prop_main', 'prop_qualifier'])
    
    for (game, team, player, prop_main, prop_qualifier), group in grouped:
        if not all([game, player]): continue
        team_logo = group['team_logo'].iloc[0] if team != 'Unknown' else ''
        if team not in output_structure[game]['teams']:
            output_structure[game]['teams'][team] = {'logo': team_logo, 'players': {}}
        player_props = output_structure[game]['teams'][team]['players'].setdefault(player, {'props': {}})
        market_data = {}
        for _, row in group.iterrows():
            market_data[row['sportsbook']] = { 'line': row['line'], 'over': int(row['over_odds']), 'under': int(row['under_odds']) }
        player_props['props'].setdefault(prop_main, {})[prop_qualifier] = market_data

    for game_name, data in output_structure.items():
        if 'Unknown' in data['teams']:
            data['teams']['Players'] = data['teams'].pop('Unknown')
            
    return props_df, output_structure, None, '6', sportsbooks

@app.route('/')
def index():
    raw_props_df, final_data, error_msg, week_number, sportsbooks = get_combined_data()
    arbitrage_ops = find_arbitrage_opportunities(raw_props_df)
    return render_template('index.html', final_data=final_data, error_msg=error_msg, week_number=week_number, arbitrage_ops=arbitrage_ops, sportsbooks=sportsbooks)

if __name__ == '__main__':
    app.run(debug=True)