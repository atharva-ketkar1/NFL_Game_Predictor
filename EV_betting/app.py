from flask import Flask, render_template
import pandas as pd
import os
import glob
from collections import defaultdict

app = Flask(__name__)

def get_latest_data():
    """Finds the latest props and game lines CSVs and combines them into a single data structure."""
    data_dir = 'nfl_data'
    if not os.path.exists(data_dir):
        return None, "Data folder 'nfl_data' not found. Please run the scraper first.", None

    props_files = glob.glob(os.path.join(data_dir, '*_props.csv'))
    lines_files = glob.glob(os.path.join(data_dir, '*_game_lines.csv'))

    if not props_files or not lines_files:
        return None, "Props or game lines CSV file not found. Please run the scraper.", None

    latest_props_file = max(props_files, key=os.path.getctime)
    latest_lines_file = max(lines_files, key=os.path.getctime)
    
    try:
        week_number = os.path.basename(latest_props_file).split('_')[3]
        props_df = pd.read_csv(latest_props_file)
        lines_df = pd.read_csv(latest_lines_file)
        props_df.fillna('', inplace=True)
        lines_df.fillna('', inplace=True)
        lines_df.set_index('game', inplace=True)

        # Group player props by Game -> Team -> Player
        player_props_grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for _, row in props_df.iterrows():
            game, team, player = row['game'], row['team_name'], row['player_name']
            if player not in player_props_grouped[game][team]:
                player_props_grouped[game][team][player] = []
            player_props_grouped[game][team][player].append(row.to_dict())

        # Combine player props and game lines into a final structure
        final_data = defaultdict(dict)
        for game, teams in player_props_grouped.items():
            team_data = {}
            for team, players in teams.items():
                first_player = list(players.keys())[0]
                logo = players[first_player][0].get('team_logo', '')
                team_data[team] = {'logo': logo, 'players': players}
            
            final_data[game]['player_props'] = team_data
            if game in lines_df.index:
                final_data[game]['game_lines'] = lines_df.loc[game].to_dict()

        return final_data, None, week_number
    except Exception as e:
        return None, f"Error processing data files: {e}", None

@app.route('/')
def index():
    final_data, error_msg, week_number = get_latest_data()
    # FIX: Pass the data with a consistent variable name that the template will use.
    return render_template('index.html', 
                           final_data=final_data, 
                           error_msg=error_msg,
                           week_number=week_number)

if __name__ == '__main__':
    app.run(debug=True)

