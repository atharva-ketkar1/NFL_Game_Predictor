import os
import re
import pandas as pd
import unicodedata
from collections import defaultdict
from flask import Flask, render_template, redirect, url_for

app = Flask(__name__)

# --- Constants & Mappings ---
TEAM_MAP = {
    'ARI Cardinals': 'Arizona Cardinals', 'ATL Falcons': 'Atlanta Falcons', 'BAL Ravens': 'Baltimore Ravens', 'BUF Bills': 'Buffalo Bills', 'CAR Panthers': 'Carolina Panthers', 'CHI Bears': 'Chicago Bears', 'CIN Bengals': 'Cincinnati Bengals', 'CLE Browns': 'Cleveland Browns', 'DAL Cowboys': 'Dallas Cowboys', 'DEN Broncos': 'Denver Broncos', 'DET Lions': 'Detroit Lions', 'GB Packers': 'Green Bay Packers', 'HOU Texans': 'Houston Texans', 'IND Colts': 'Indianapolis Colts', 'JAX Jaguars': 'Jacksonville Jaguars', 'KC Chiefs': 'Kansas City Chiefs', 'LV Raiders': 'Las Vegas Raiders', 'LA Chargers': 'Los Angeles Chargers', 'LA Rams': 'Los Angeles Rams', 'MIA Dolphins': 'Miami Dolphins', 'MIN Vikings': 'Minnesota Vikings', 'NE Patriots': 'New England Patriots', 'NO Saints': 'New Orleans Saints', 'NY Giants': 'New York Giants', 'NY Jets': 'New York Jets', 'PHI Eagles': 'Philadelphia Eagles', 'PIT Steelers': 'Pittsburgh Steelers', 'SF 49ers': 'San Francisco 49ers', 'SEA Seahawks': 'Seattle Seahawks', 'TB Buccaneers': 'Tampa Bay Buccaneers', 'TEN Titans': 'Tennessee Titans', 'WAS Commanders': 'Washington Commanders'
}

# --- CORRECTED MAPPING: Using underscores for FanDuel Logo URLs ---
FANDUEL_LOGO_MAP = {
    'Arizona Cardinals': 'arizona_cardinals', 'Atlanta Falcons': 'atlanta_falcons', 'Baltimore Ravens': 'baltimore_ravens', 'Buffalo Bills': 'buffalo_bills', 'Carolina Panthers': 'carolina_panthers', 'Chicago Bears': 'chicago_bears', 'Cincinnati Bengals': 'cincinnati_bengals', 'Cleveland Browns': 'cleveland_browns', 'Dallas Cowboys': 'dallas_cowboys', 'Denver Broncos': 'denver_broncos', 'Detroit Lions': 'detroit_lions', 'Green Bay Packers': 'green_bay_packers', 'Houston Texans': 'houston_texans', 'Indianapolis Colts': 'indianapolis_colts', 'Jacksonville Jaguars': 'jacksonville_jaguars', 'Kansas City Chiefs': 'kansas_city_chiefs', 'Las Vegas Raiders': 'las_vegas_raiders', 'Los Angeles Chargers': 'los_angeles_chargers', 'Los Angeles Rams': 'los_angeles_rams', 'Miami Dolphins': 'miami_dolphins', 'Minnesota Vikings': 'minnesota_vikings', 'New England Patriots': 'new_england_patriots', 'New Orleans Saints': 'new_orleans_saints', 'New York Giants': 'new_york_giants', 'New York Jets': 'new_york_jets', 'Philadelphia Eagles': 'philadelphia_eagles', 'Pittsburgh Steelers': 'pittsburgh_steelers', 'San Francisco 49ers': 'san_francisco_49ers', 'Seattle Seahawks': 'seattle_seahawks', 'Tampa Bay Buccaneers': 'tampa_bay_buccaneers', 'Tennessee Titans': 'tennessee_titans', 'Washington Commanders': 'washington_commanders'
}


# --- Helper Functions ---
def get_available_weeks(data_dir):
    """Scans the data directory for week folders (e.g., 'week_1') and returns a sorted list of week numbers."""
    weeks = []
    if not os.path.isdir(data_dir):
        return []
    for item in os.listdir(data_dir):
        if os.path.isdir(os.path.join(data_dir, item)):
            match = re.match(r'^week_(\d+)$', item)
            if match:
                weeks.append(int(match.group(1)))
    weeks.sort(reverse=True) # Sort with the latest week first
    return weeks

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

def parse_prop_type(prop_string: str) -> dict:
    prop_string = str(prop_string).strip()
    if not prop_string:
        return {'main': 'Unknown Prop', 'qualifier': ''}
    PROP_TYPE_MAP = {
        'Passing Touchdowns': 'Passing Touchdowns', 'Passing TDs': 'Passing Touchdowns', 'Pass TDs': 'Passing Touchdowns', 'TDs': 'Passing Touchdowns',
        'Passing Yards': 'Passing Yards', 'Passing Yds': 'Passing Yards', 'Pass Yds': 'Passing Yards', 'Yds': 'Passing Yards',
        'Passing Completions': 'Passing Completions', 'Completions': 'Passing Completions', 'Pass Completions': 'Passing Completions', 'Passing': 'Passing Completions',
        'Passing Attempts': 'Passing Attempts', 'Pass Attempts': 'Passing Attempts',
        'Interceptions Thrown': 'Interceptions Thrown', 'Interceptions': 'Interceptions Thrown', 'Interception': 'Interceptions Thrown',
        'Receiving Yards': 'Receiving Yards', 'Receiving Yds': 'Receiving Yards', 'Rec Yards': 'Receiving Yards',
        'Receptions': 'Receptions', 'Total Receptions': 'Receptions', 'Reception': 'Receptions',
        'Rushing Yards': 'Rushing Yards', 'Rushing Yds': 'Rushing Yards', 'Rush Yards': 'Rushing Yards',
        'Rushing Attempts': 'Rushing Attempts', 'Rush Attempts': 'Rushing Attempts',
        'Rushing + Receiving Yards': 'Rushing + Receiving Yards', 'Rushing + Receiving Yds': 'Rushing + Receiving Yards', 'Rush + Rec Yards': 'Rushing + Receiving Yards',
        'Passing + Rushing Yards': 'Passing + Rushing Yards', 'Passing + Rushing Yds': 'Passing + Rushing Yards',
        'Field Goals Made': 'Field Goals Made', 'FG Made': 'Field Goals Made',
        'Kicking Points': 'Kicking Points', 'Kicking Pts': 'Kicking Points',
        'Extra Points Made': 'Extra Points Made', 'PAT Made': 'Extra Points Made',
        'Fantasy Points': 'Fantasy Points', 'WR/TE Fantasy Points': 'Fantasy Points', 'RB Fantasy Points': 'Fantasy Points', 'QB Fantasy Points': 'Fantasy Points',
    }
    qualifiers = { ' - 1st Half': '1st Half', ' - 1H': '1st Half', ' - 1st Quarter': '1st Quarter', ' - 1Q': '1st Quarter', '1st Qtr': '1st Quarter', 'Longest': 'Longest' }
    prop_qualifier = 'Full Game'; main_prop_str = prop_string
    if 'Longest' in main_prop_str:
        prop_qualifier = 'Longest'; main_prop_str = main_prop_str.replace('Longest', '').strip()
    else:
        for key, val in qualifiers.items():
            if key in main_prop_str:
                prop_qualifier = val; main_prop_str = main_prop_str.replace(key, '').strip(); break
    main_prop_str = main_prop_str.replace(' O/U', '').strip()
    main_prop = PROP_TYPE_MAP.get(main_prop_str, main_prop_str)
    if prop_qualifier == 'Longest':
        if main_prop in ['Receiving Yards', 'Receptions']: main_prop = 'Longest Reception'
        elif main_prop in ['Rushing Yards', 'Rush']: main_prop = 'Longest Rush'
        elif main_prop in ['Passing Completions', 'Pass']: main_prop = 'Longest Completion'
        prop_qualifier = ''
    return {'main': main_prop, 'qualifier': prop_qualifier}

def extract_player_name(text, known_players):
    text = str(text)
    best_match = ''
    for player in sorted(known_players, key=len, reverse=True):
        if text.lower().startswith(player.lower()):
            best_match = player; break
    return best_match if best_match else text

def get_combined_data(week_number):
    output_structure = defaultdict(lambda: {'game_lines': None, 'teams': {}})
    sportsbooks = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    week_folder = f'week_{week_number}'
    prop_files = {
        'fanduel': os.path.join(week_folder, f'fanduel_nfl_week_{week_number}_props.csv'),
        'draftkings': os.path.join(week_folder, f'draftkings_nfl_week_{week_number}_props.csv')
    }
    if not os.path.isdir(data_dir):
        return None, output_structure, f"Error: Data directory not found at '{os.path.abspath(data_dir)}'.", str(week_number), sportsbooks
    
    fanduel_path = os.path.join(data_dir, prop_files['fanduel'])
    draftkings_path = os.path.join(data_dir, prop_files['draftkings'])
    fanduel_df = pd.read_csv(fanduel_path) if os.path.exists(fanduel_path) else pd.DataFrame()
    draftkings_df = pd.read_csv(draftkings_path) if os.path.exists(draftkings_path) else pd.DataFrame()
    
    if fanduel_df.empty and draftkings_df.empty:
        return None, output_structure, f"No prop data files found for Week {week_number} in '{os.path.abspath(data_dir)}'.", str(week_number), sportsbooks

    if not fanduel_df.empty: fanduel_df['sportsbook'] = 'Fanduel'
    if not draftkings_df.empty:
        draftkings_df.rename(columns={'player': 'player_name'}, inplace=True)
        draftkings_df['sportsbook'] = 'Draftkings'
            
    props_df = pd.concat([fanduel_df, draftkings_df], ignore_index=True)
    props_df.rename(columns={'over': 'over_odds', 'under': 'under_odds'}, inplace=True)

    # --- FIX: Convert odds columns to a numeric type to prevent TypeError ---
    for col in ['over_odds', 'under_odds']:
        if col in props_df.columns:
            props_df[col] = pd.to_numeric(
                props_df[col].astype(str).str.replace('−', '-'), 
                errors='coerce'
            )

    # --- NEW UNIFIED NAME LOGIC ---
    props_df['player_name_norm'] = props_df['player_name'].apply(normalize_player_name)

    canonical_name_map = {}
    if not fanduel_df.empty:
        fd_map_df = fanduel_df[['player_name']].dropna().copy()
        fd_map_df['player_name_norm'] = fd_map_df['player_name'].apply(normalize_player_name)
        canonical_name_map = fd_map_df.drop_duplicates('player_name_norm', keep='last').set_index('player_name_norm')['player_name'].to_dict()

    props_df['player_name'] = props_df['player_name_norm'].map(canonical_name_map).fillna(props_df['player_name'])
    
    # --- REVISED PROP PARSING LOGIC ---
    def get_prop_string_from_row(row):
        prop_str = str(row['prop_type'])
        player_str = str(row['player_name'])
        if prop_str.lower().startswith(player_str.lower()):
            return prop_str[len(player_str):].strip()
        return prop_str

    clean_prop_strings = props_df.apply(get_prop_string_from_row, axis=1)
    prop_details = clean_prop_strings.apply(parse_prop_type)
    
    props_df['prop_main'] = prop_details.apply(lambda x: x['main'])
    props_df['prop_qualifier'] = prop_details.apply(lambda x: x['qualifier'])

    # --- Build Player-Team Map for logos and grouping ---
    player_team_map = {}
    if not fanduel_df.empty and 'team_name' in fanduel_df.columns:
        map_source_df = fanduel_df[['player_name', 'team_name']].dropna().copy()
        map_source_df['player_name_norm'] = map_source_df['player_name'].apply(normalize_player_name)
        map_source_df = map_source_df.drop_duplicates(subset=['player_name_norm'], keep='last')
        player_team_map = map_source_df.set_index('player_name_norm')['team_name'].to_dict()

    # Ensure team_name column exists and fill it using the map
    if 'team_name' not in props_df.columns:
        props_df['team_name'] = ''
    props_df['team_name'] = props_df['player_name_norm'].map(player_team_map).fillna(props_df.get('team_name', ''))

    props_df.dropna(subset=['game', 'player_name', 'over_odds', 'under_odds', 'prop_main'], inplace=True)
    
    props_df['game_norm'] = props_df['game'].astype(str).apply(lambda g: normalize_game_name(g, TEAM_MAP))
    
    sportsbooks = sorted(props_df['sportsbook'].unique())
    props_df['grouping_team'] = props_df['team_name'].replace('', 'Unknown')
    
    grouped = props_df.groupby(['game_norm', 'grouping_team', 'player_name', 'prop_main', 'prop_qualifier'])
    
    for (game, team, player, prop_main, prop_qualifier), group in grouped:
        if not all([game, player]): continue
        
        team_logo_url = ''
        if team != 'Unknown':
            team_logo_slug = FANDUEL_LOGO_MAP.get(team, '')
            if team_logo_slug:
                team_logo_url = f"https://assets.sportsbook.fanduel.com/images/team/nfl/{team_logo_slug}.png"

        if team not in output_structure[game]['teams']:
            output_structure[game]['teams'][team] = {'logo': team_logo_url, 'players': {}}
        player_props = output_structure[game]['teams'][team]['players'].setdefault(player, {'props': {}})
        market_data = {}
        for _, row in group.iterrows():
            def format_odds(odds):
                try: return f"+{int(odds)}" if int(odds) > 0 else str(int(odds));
                except: return str(odds)
            market_data[row['sportsbook']] = {'line': row['line'], 'over': format_odds(row['over_odds']), 'under': format_odds(row['under_odds'])}
        player_props['props'].setdefault(prop_main, {})[prop_qualifier] = market_data

    for game_name, data in output_structure.items():
        if 'Unknown' in data['teams']: data['teams']['Players'] = data['teams'].pop('Unknown')
            
    return props_df, output_structure, None, str(week_number), sportsbooks


@app.route('/')
def index():
    """Redirects to the page for the most recent week."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    available_weeks = get_available_weeks(data_dir)
    if not available_weeks:
        return render_template('index.html', error_msg="No weekly data found in the 'nfl_data' directory.", final_data={}, available_weeks=[], sportsbooks=[])
    latest_week = available_weeks[0]
    return redirect(url_for('show_week', week_num=latest_week))

@app.route('/week/<int:week_num>')
def show_week(week_num):
    """Displays the dashboard for a specific week."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    available_weeks = get_available_weeks(data_dir)
    if week_num not in available_weeks:
        return redirect(url_for('index'))

    raw_props_df, final_data, error_msg, week_number, sportsbooks = get_combined_data(week_num)
    arbitrage_ops = find_arbitrage_opportunities(raw_props_df)

    return render_template('index.html', 
                           final_data=final_data, 
                           error_msg=error_msg, 
                           week_number=week_number, 
                           arbitrage_ops=arbitrage_ops, 
                           sportsbooks=sportsbooks,
                           available_weeks=available_weeks,
                           current_week=week_num)

if __name__ == '__main__':
    app.run(debug=True)

