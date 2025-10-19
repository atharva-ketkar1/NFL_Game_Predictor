import os
import re
import pandas as pd
import unicodedata
from collections import defaultdict
from flask import Flask, render_template, redirect, url_for, request

app = Flask(__name__)

# --- Constants & Mappings ---
TEAM_MAP = {
    'ARI Cardinals': 'Arizona Cardinals', 'ATL Falcons': 'Atlanta Falcons', 'BAL Ravens': 'Baltimore Ravens', 'BUF Bills': 'Buffalo Bills', 'CAR Panthers': 'Carolina Panthers', 'CHI Bears': 'Chicago Bears', 'CIN Bengals': 'Cincinnati Bengals', 'CLE Browns': 'Cleveland Browns', 'DAL Cowboys': 'Dallas Cowboys', 'DEN Broncos': 'Denver Broncos', 'DET Lions': 'Detroit Lions', 'GB Packers': 'Green Bay Packers', 'HOU Texans': 'Houston Texans', 'IND Colts': 'Indianapolis Colts', 'JAX Jaguars': 'Jacksonville Jaguars', 'KC Chiefs': 'Kansas City Chiefs', 'LV Raiders': 'Las Vegas Raiders', 'LA Chargers': 'Los Angeles Chargers', 'LA Rams': 'Los Angeles Rams', 'MIA Dolphins': 'Miami Dolphins', 'MIN Vikings': 'Minnesota Vikings', 'NE Patriots': 'New England Patriots', 'NO Saints': 'New Orleans Saints', 'NY Giants': 'New York Giants', 'NY Jets': 'New York Jets', 'PHI Eagles': 'Philadelphia Eagles', 'PIT Steelers': 'Pittsburgh Steelers', 'SF 49ers': 'San Francisco 49ers', 'SEA Seahawks': 'Seattle Seahawks', 'TB Buccaneers': 'Tampa Bay Buccaneers', 'TEN Titans': 'Tennessee Titans', 'WAS Commanders': 'Washington Commanders'
}

FANDUEL_LOGO_MAP = {
    'Arizona Cardinals': 'arizona_cardinals', 'Atlanta Falcons': 'atlanta_falcons', 'Baltimore Ravens': 'baltimore_ravens', 'Buffalo Bills': 'buffalo_bills', 'Carolina Panthers': 'carolina_panthers', 'Chicago Bears': 'chicago_bears', 'Cincinnati Bengals': 'cincinnati_bengals', 'Cleveland Browns': 'cleveland_browns', 'Dallas Cowboys': 'dallas_cowboys', 'Denver Broncos': 'denver_broncos', 'Detroit Lions': 'detroit_lions', 'Green Bay Packers': 'green_bay_packers', 'Houston Texans': 'houston_texans', 'Indianapolis Colts': 'indianapolis_colts', 'Jacksonville Jaguars': 'jacksonville_jaguar', 'Kansas City Chiefs': 'kansas_city_chiefs', 'Las Vegas Raiders': 'las_vegas_raiders', 'Los Angeles Chargers': 'los_angeles_chargers', 'Los Angeles Rams': 'los_angeles_rams', 'Miami Dolphins': 'miami_dolphins', 'Minnesota Vikings': 'minnesota_vikings', 'New England Patriots': 'new_england_patriots', 'New Orleans Saints': 'new_orleans_saints', 'New York Giants': 'new_york_giants', 'New York Jets': 'new_york_jets', 'Philadelphia Eagles': 'philadelphia_eagles', 'Pittsburgh Steelers': 'pittsburgh_steelers', 'San Francisco 49ers': 'san_francisco_49ers', 'Seattle Seahawks': 'seattle_seahawks', 'Tampa Bay Buccaneers': 'tampa_bay_buccaneers', 'Tennessee Titans': 'tennessee_titans', 'Washington Commanders': 'washington_commanders'
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

ODDS_DIFF_THRESHOLD = 20 # Minimum difference in odds (e.g., -110 vs -130) to be flagged
LINE_DIFF_THRESHOLDS = {
    'Passing Yards': 10.0,
    'Rushing Yards': 5.0,
    'Receiving Yards': 5.0,
    'Rushing + Receiving Yards': 5.0,
    'Passing + Rushing Yards': 10.0,
    'Receptions': 1.0,
    'Passing Completions': 1.0,
    'Passing Attempts': 2.0,
    'Rushing Attempts': 2.0,
    'Passing Touchdowns': 0.5,
    'Interceptions Thrown': 0.5,
    'Field Goals Made': 0.5,
    'Kicking Points': 1.0
}
DEFAULT_LINE_THRESHOLD = 1.0

def find_value_bets(props_df):
    """
    Finds two types of value opportunities:
    1. Odds Shopping: Same prop/line, but significant odds differences.
    2. Line Shopping: Same prop, but different lines (middling opportunity).
    """
    if props_df is None or props_df.empty:
        return {'odds_shopping': [], 'line_shopping': []}

    df = props_df.copy()
    df.dropna(subset=['over_odds', 'under_odds', 'line', 'player_name_norm', 'prop_main', 'prop_qualifier'], inplace=True)

    odds_ops = []
    line_ops = []

    grouped = df.groupby(['player_name_norm', 'prop_main', 'prop_qualifier'])

    for (player_norm, prop_main, prop_qual), group in grouped:
        if len(group['sportsbook'].unique()) < 2:
            continue # Need at least two books to compare

        player_name = group['player_name'].iloc[0]
        prop_display = f"{prop_main} ({prop_qual})" if prop_qual and prop_qual != 'Full Game' else prop_main

        # --- Logic 1: Odds Shopping (Same Line, Different Odds) ---
        line_grouped = group.groupby('line')
        for line, line_group in line_grouped:
            if len(line_group['sportsbook'].unique()) < 2:
                continue

            # Check OVERs for value
            overs = line_group.dropna(subset=['over_odds'])
            if len(overs) > 1:
                best_over = overs.loc[overs['over_odds'].idxmax()]
                worst_over = overs.loc[overs['over_odds'].idxmin()]
                diff = best_over['over_odds'] - worst_over['over_odds']
                if diff >= ODDS_DIFF_THRESHOLD:
                    odds_ops.append({
                        'type': 'Over',
                        'player_name': player_name,
                        'prop_type': prop_display,
                        'line': line,
                        'best_book': best_over['sportsbook'],
                        'best_odds': int(best_over['over_odds']),
                        'worst_book': worst_over['sportsbook'],
                        'worst_odds': int(worst_over['over_odds']),
                        'diff': int(diff)
                    })
            
            # Check UNDERs for value
            unders = line_group.dropna(subset=['under_odds'])
            if len(unders) > 1:
                best_under = unders.loc[unders['under_odds'].idxmax()]
                worst_under = unders.loc[unders['under_odds'].idxmin()]
                diff = best_under['under_odds'] - worst_under['under_odds']
                if diff >= ODDS_DIFF_THRESHOLD:
                    odds_ops.append({
                        'type': 'Under',
                        'player_name': player_name,
                        'prop_type': prop_display,
                        'line': line,
                        'best_book': best_under['sportsbook'],
                        'best_odds': int(best_under['under_odds']),
                        'worst_book': worst_under['sportsbook'],
                        'worst_odds': int(worst_under['under_odds']),
                        'diff': int(diff)
                    })

        # --- Logic 2: Line Shopping (Different Lines, Same Prop) ---
        unique_lines = group['line'].dropna().unique()
        if len(unique_lines) > 1:
            try:
                max_line_row = group.loc[group['line'].idxmax()]
                min_line_row = group.loc[group['line'].idxmin()]
                
                line_diff = max_line_row['line'] - min_line_row['line']
                threshold = LINE_DIFF_THRESHOLDS.get(prop_main, DEFAULT_LINE_THRESHOLD)

                if line_diff >= threshold:
                    def format_odds(odds):
                        try: return f"+{int(odds)}" if int(odds) > 0 else str(int(odds));
                        except: return str(odds)
                    
                    line_ops.append({
                        'player_name': player_name,
                        'prop_type': prop_display,
                        'bet_over_book': min_line_row['sportsbook'],
                        'bet_over_line': min_line_row['line'],
                        'bet_over_odds': format_odds(min_line_row['over_odds']),
                        'bet_under_book': max_line_row['sportsbook'],
                        'bet_under_line': max_line_row['line'],
                        'bet_under_odds': format_odds(max_line_row['under_odds']),
                        'line_diff': line_diff
                    })
            except Exception as e:
                print(f"Error processing line shopping for {player_name} - {prop_display}: {e}")

    # Sort lists for better display (e.g., biggest diffs first)
    odds_ops.sort(key=lambda x: x['diff'], reverse=True)
    line_ops.sort(key=lambda x: x['line_diff'], reverse=True)
    
    # --- (MODIFIED) Limit to the top 25 for each category ---
    top_odds_ops = odds_ops[:25]
    top_line_ops = line_ops[:25]
    
    return {'odds_shopping': top_odds_ops, 'line_shopping': top_line_ops}


def find_biggest_line_moves(props_df):
    """
    Finds the props with the largest line movement from their first recorded
    point to their last, grouped by player, prop, and sportsbook.
    """
    if props_df is None or props_df.empty or 'scrape_timestamp' not in props_df.columns:
        return []

    df = props_df.copy()
    
    # Ensure timestamp is datetime for proper sorting
    df['scrape_timestamp'] = pd.to_datetime(df['scrape_timestamp'])
    
    # We group by the unique prop AND the sportsbook, as lines move
    # independently on different books.
    grouped = df.groupby(['player_name_norm', 'prop_main', 'prop_qualifier', 'sportsbook'])

    moves = []

    for (player_norm, prop_main, prop_qual, sportsbook), group in grouped:
        if len(group) < 2:
            continue # Need at least two data points to show movement

        # Sort by time to find the first and last entry
        group = group.sort_values('scrape_timestamp')
        
        start_row = group.iloc[0]
        end_row = group.iloc[-1]

        start_line = start_row['line']
        end_line = end_row['line']

        line_change = end_line - start_line

        # Use the same thresholds as the "middles" to find significant moves
        threshold = LINE_DIFF_THRESHOLDS.get(prop_main, DEFAULT_LINE_THRESHOLD)

        if abs(line_change) >= threshold:
            player_name = start_row['player_name']
            prop_display = f"{prop_main} ({prop_qual})" if prop_qual and prop_qual != 'Full Game' else prop_main

            moves.append({
                'player_name': player_name,
                'prop_type': prop_display,
                'sportsbook': sportsbook,
                'start_line': start_line,
                'end_line': end_line,
                'line_change': line_change,
                'start_time': start_row['scrape_timestamp'].strftime('%a, %b %d %I:%M%p'),
                'end_time': end_row['scrape_timestamp'].strftime('%a, %b %d %I:%M%p'),
                'abs_change': abs(line_change), # Helper for sorting
                # (NEW) Add keys for history lookup
                'player_name_norm': player_norm,
                'prop_main': prop_main,
                'prop_qualifier': prop_qual,
            })

    # Sort the final list by the largest absolute change
    moves.sort(key=lambda x: x['abs_change'], reverse=True)
    
    # --- (MODIFIED) Return only the top 25 biggest moves ---
    return moves[:25]


def parse_prop_type(prop_string: str) -> dict:
    prop_string = str(prop_string).strip()
    if not prop_string:
        return {'main': 'Unknown Prop', 'qualifier': ''}
    
    # --- THIS IS THE CORRECTED DICTIONARY ---
    PROP_TYPE_MAP = {
        # Passing Touchdowns
        'Passing Touchdowns': 'Passing Touchdowns', 'Passing TDs': 'Passing Touchdowns', 'Pass TDs': 'Passing Touchdowns', 'TDs': 'Passing Touchdowns',
        
        # Passing Yards (FIXED)
        'Passing Yards': 'Passing Yards', 'Passing Yds': 'Passing Yards', 'Pass Yds': 'Passing Yards', 'Pass Yards': 'Passing Yards', 'Yds': 'Passing Yards',
        
        # Passing Completions (FIXED)
        'Passing Completions': 'Passing Completions', 'Completions': 'Passing Completions', 'Pass Completions': 'Passing Completions', 'Passing Completion': 'Passing Completions', 'Passing': 'Passing Completions',
        
        # Passing Attempts
        'Passing Attempts': 'Passing Attempts', 'Pass Attempts': 'Passing Attempts',
        
        # Interceptions
        'Interceptions Thrown': 'Interceptions Thrown', 'Interceptions': 'Interceptions Thrown', 'Interception': 'Interceptions Thrown',
        
        # Receiving Yards (FIXED)
        'Receiving Yards': 'Receiving Yards', 'Receiving Yds': 'Receiving Yards', 'Rec Yards': 'Receiving Yards', 'Rec Yds': 'Receiving Yards',
        
        # Receptions
        'Receptions': 'Receptions', 'Total Receptions': 'Receptions', 'Reception': 'Receptions',
        
        # Rushing Yards (FIXED)
        'Rushing Yards': 'Rushing Yards', 'Rushing Yds': 'Rushing Yards', 'Rush Yards': 'Rushing Yards', 'Rush Yds': 'Rushing Yards',
        
        # Rushing Attempts
        'Rushing Attempts': 'Rushing Attempts', 'Rush Attempts': 'Rushing Attempts',
        
        # Combo Props (Rushing + Receiving) (FIXED)
        'Rushing + Receiving Yards': 'Rushing + Receiving Yards', 'Rushing + Receiving Yds': 'Rushing + Receiving Yards', 'Rush + Rec Yards': 'Rushing + Receiving Yards', 'Rush + Rec Yds': 'Rushing + Receiving Yards',
        
        # Combo Props (Passing + Rushing) (FIXED)
        'Passing + Rushing Yards': 'Passing + Rushing Yards', 'Passing + Rushing Yds': 'Passing + Rushing Yards', 'Pass + Rush Yards': 'Passing + Rushing Yards',
        
        # Kicking
        'Field Goals Made': 'Field Goals Made', 'FG Made': 'Field Goals Made',
        'Kicking Points': 'Kicking Points', 'Kicking Pts': 'Kicking Points',
        'Extra Points Made': 'Extra Points Made', 'PAT Made': 'Extra Points Made',
        
        # Fantasy
        'Fantasy Points': 'Fantasy Points', 'WR/TE Fantasy Points': 'Fantasy Points', 'RB Fantasy Points': 'Fantasy Points', 'QB Fantasy Points': 'Fantasy Points',
    }
    # --- END OF CORRECTIONS ---
    
    qualifiers = { ' - 1st Half': '1st Half', ' - 1H': '1st Half', ' - 1st Quarter': '1st Quarter', ' - 1Q': '1st Quarter', '1st Qtr': '1st Quarter', 'Longest': 'Longest' }
    prop_qualifier = 'Full Game'; main_prop_str = prop_string
    if 'Longest' in main_prop_str:
        prop_qualifier = 'Longest'; main_prop_str = main_prop_str.replace('Longest', '').strip()
    else:
        for key, val in qualifiers.items():
            if key in main_prop_str:
                prop_qualifier = val; main_prop_str = main_prop_str.replace(key, '').strip(); break
    main_prop_str = main_prop_str.replace(' O/U', '').strip()
    
    # This line now correctly maps all variations to one canonical name
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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    week_folder = f'week_{week_number}'
    
    week_path = os.path.join(data_dir, week_folder)

    if not os.path.isdir(data_dir):
        return None, f"Error: Base data directory not found at '{os.path.abspath(data_dir)}'.", []
    
    if not os.path.isdir(week_path):
         return None, f"Error: Week directory not found at '{os.path.abspath(week_path)}'.", []

    fd_history_path = os.path.join(week_path, f'fanduel_nfl_week_{week_number}_props_history.csv')
    dk_history_path = os.path.join(week_path, f'draftkings_nfl_week_{week_number}_props_history.csv')
    fd_legacy_path = os.path.join(week_path, f'fanduel_nfl_week_{week_number}_props.csv')
    dk_legacy_path = os.path.join(week_path, f'draftkings_nfl_week_{week_number}_props.csv')

    fanduel_path_to_load = None
    if os.path.exists(fd_history_path):
        fanduel_path_to_load = fd_history_path
    elif os.path.exists(fd_legacy_path):
        fanduel_path_to_load = fd_legacy_path
        
    draftkings_path_to_load = None
    if os.path.exists(dk_history_path):
        draftkings_path_to_load = dk_history_path
    elif os.path.exists(dk_legacy_path):
        draftkings_path_to_load = dk_legacy_path

    fanduel_df = pd.read_csv(fanduel_path_to_load) if fanduel_path_to_load else pd.DataFrame()
    draftkings_df = pd.read_csv(draftkings_path_to_load) if draftkings_path_to_load else pd.DataFrame()
    
    if fanduel_df.empty and draftkings_df.empty:
        error_msg = f"No prop data files (e.g., ..._props.csv or ..._props_history.csv) found for Week {week_number} in '{os.path.abspath(week_path)}'."
        return None, error_msg, []

    if not fanduel_df.empty: fanduel_df['sportsbook'] = 'Fanduel'
    if not draftkings_df.empty:
        draftkings_df.rename(columns={'player': 'player_name'}, inplace=True)
        draftkings_df['sportsbook'] = 'Draftkings'

    props_df = pd.concat([fanduel_df, draftkings_df], ignore_index=True)
    props_df.rename(columns={'over': 'over_odds', 'under': 'under_odds'}, inplace=True)

    for col in ['over_odds', 'under_odds']:
        if col in props_df.columns:
            props_df[col] = pd.to_numeric(
                props_df[col].astype(str).str.replace('−', '-'),
                errors='coerce'
            )

    if not fanduel_df.empty:
        known_clean_players = set(fanduel_df['player_name'].dropna().unique())
        props_df['player_name'] = props_df['player_name'].apply(
            lambda name: extract_player_name(name, known_clean_players)
        )

    props_df['player_name_norm'] = props_df['player_name'].apply(normalize_player_name)

    canonical_name_map = {}
    if not fanduel_df.empty:
        fd_map_df = fanduel_df[['player_name']].dropna().copy()
        fd_map_df['player_name_norm'] = fd_map_df['player_name'].apply(normalize_player_name)
        canonical_name_map = fd_map_df.drop_duplicates('player_name_norm', keep='last').set_index('player_name_norm')['player_name'].to_dict()

    props_df['player_name'] = props_df['player_name_norm'].map(canonical_name_map).fillna(props_df['player_name'])

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

    player_team_map = {}
    if not fanduel_df.empty and 'team_name' in fanduel_df.columns:
        map_source_df = fanduel_df[['player_name', 'team_name']].dropna().copy()
        map_source_df['player_name_norm'] = map_source_df['player_name'].apply(normalize_player_name)
        map_source_df = map_source_df.drop_duplicates(subset=['player_name_norm'], keep='last')
        player_team_map = map_source_df.set_index('player_name_norm')['team_name'].to_dict()

    if 'team_name' not in props_df.columns:
        props_df['team_name'] = ''
    props_df['team_name'] = props_df['player_name_norm'].map(player_team_map).fillna(props_df.get('team_name', ''))

    props_df.dropna(subset=['game', 'player_name', 'over_odds', 'under_odds', 'prop_main'], inplace=True)

    props_df['game_norm'] = props_df['game'].astype(str).apply(lambda g: normalize_game_name(g, TEAM_MAP))

    sportsbooks = sorted(props_df['sportsbook'].unique())
    props_df['grouping_team'] = props_df['team_name'].replace('', 'Unknown')

    return props_df, None, sportsbooks


def structure_props_for_template(props_df, history_map): # MODIFIED SIGNATURE
    """Takes a DataFrame of props and structures it into a nested dict for the template."""
    output_structure = defaultdict(lambda: {'game_lines': None, 'teams': {}})
    if props_df is None or props_df.empty:
        return output_structure

    grouped = props_df.groupby(['game_norm', 'grouping_team', 'player_name', 'prop_main', 'prop_qualifier', 'player_name_norm'])

    for (game, team, player, prop_main, prop_qualifier, player_norm), group in grouped: # ADDED player_norm
        if not all([game, player]): continue

        team_logo_url = ''
        if team != 'Unknown':
            team_logo_slug = FANDUEL_LOGO_MAP.get(team, '')
            if team_logo_slug:
                team_logo_url = f"https://assets.sportsbook.fanduel.com/images/team/nfl/{team_logo_slug}.png"

        if team not in output_structure[game]['teams']:
            output_structure[game]['teams'][team] = {'logo': team_logo_url, 'players': {}}
        player_props = output_structure[game]['teams'][team]['players'].setdefault(player, {'props': {}})
        
        prop_history_json = history_map.get((player_norm, prop_main, prop_qualifier), '[]') # Default to empty JSON array
        
        market_data = {}
        for _, row in group.iterrows():
            def format_odds(odds):
                try: return f"+{int(odds)}" if int(odds) > 0 else str(int(odds));
                except: return str(odds)
            
            market_data[row['sportsbook']] = {
                'line': row['line'], 
                'over': format_odds(row['over_odds']), 
                'under': format_odds(row['under_odds']),
                'history': prop_history_json # Attach the SAME history JSON to all books for this prop
            }
        
        player_props['props'].setdefault(prop_main, {})[prop_qualifier] = market_data

    for game_name, data in output_structure.items():
        if 'Unknown' in data['teams']: data['teams']['Players'] = data['teams'].pop('Unknown')

    return output_structure


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
    player_search = request.args.get('player_search', '').strip()
    prop_filter = request.args.get('prop_filter', '').strip()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'nfl_data')
    available_weeks = get_available_weeks(data_dir)
    if week_num not in available_weeks:
        return redirect(url_for('index'))

    # Get all raw data (could be history or legacy)
    raw_historical_df, error_msg, sportsbooks = get_combined_data(week_num)

    # (NEW) Initialize biggest_moves
    biggest_moves = []

    # Handle errors or no data
    if error_msg or raw_historical_df is None or raw_historical_df.empty:
         return render_template('index.html',
                           final_data={},
                           error_msg=error_msg or "No data available for this week.",
                           week_number=str(week_num),
                           arbitrage_ops=[],
                           value_bets={'odds_shopping': [], 'line_shopping': []},
                           biggest_moves=biggest_moves, # ADDED
                           sportsbooks=[],
                           available_weeks=available_weeks,
                           current_week=week_num,
                           prop_types=[],
                           player_search=player_search,
                           prop_filter=prop_filter)

    # --- MODIFIED: Handle both history and legacy files ---
    history_map = {}
    latest_props_df = None

    if 'scrape_timestamp' in raw_historical_df.columns:
        # --- A) NEW LOGIC: File has history (Week 7+) ---
        
        # (NEW) Find biggest line moves using the FULL history
        biggest_moves = find_biggest_line_moves(raw_historical_df)

        # 1. Pre-process history map
        raw_historical_df['scrape_timestamp'] = pd.to_datetime(raw_historical_df['scrape_timestamp'])
        history_cols = ['scrape_timestamp', 'line', 'over_odds', 'under_odds', 'sportsbook']
        valid_history_cols = [col for col in history_cols if col in raw_historical_df.columns]
        history_groups = raw_historical_df.groupby(['player_name_norm', 'prop_main', 'prop_qualifier'])
        
        for (player_norm, prop_main, prop_qual), group in history_groups:
            history_json = group[valid_history_cols].to_json(orient='records', date_format='iso')
            history_map[(player_norm, prop_main, prop_qual)] = history_json

        # (NEW) Inject history JSON into biggest_moves
        for move in biggest_moves:
            history_key = (move['player_name_norm'], move['prop_main'], move['prop_qualifier'])
            move['history_json'] = history_map.get(history_key, '[]')

        # 2. Filter to get ONLY the latest props
        group_keys = ['player_name_norm', 'prop_main', 'prop_qualifier', 'line', 'sportsbook', 'game_norm']
        latest_props_df = raw_historical_df.sort_values('scrape_timestamp') \
                                           .groupby(group_keys) \
                                           .last() \
                                           .reset_index()
    else:
        # --- B) FALLBACK LOGIC: File is legacy (Week 6) ---
        # The raw data *is* the latest data, and history map remains empty.
        latest_props_df = raw_historical_df
        # history_map is already {}
        # biggest_moves is already []
        
    # --- END MODIFICATION ---

    # 1. Get unique prop types for the filter dropdown (from latest data)
    prop_types = sorted(latest_props_df['prop_main'].unique())

    # 2. Find arbitrage opportunities on the latest dataset
    arbitrage_ops = find_arbitrage_opportunities(latest_props_df)

    # 3. Find value bets / line discrepancies
    value_bets = find_value_bets(latest_props_df)

    # 4. Structure the LATEST data for the template, passing the (possibly empty) history map
    final_data = structure_props_for_template(latest_props_df, history_map)

    return render_template('index.html',
                           final_data=final_data,
                           error_msg=None,
                           week_number=str(week_num),
                           arbitrage_ops=arbitrage_ops,
                           value_bets=value_bets,
                           biggest_moves=biggest_moves, # ADDED
                           sportsbooks=sportsbooks,
                           available_weeks=available_weeks,
                           current_week=week_num,
                           prop_types=prop_types,
                           player_search=player_search,
                           prop_filter=prop_filter)


if __name__ == '__main__':
    app.run(debug=True)