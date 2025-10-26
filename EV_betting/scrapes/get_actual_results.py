import requests
import csv
import os
from datetime import datetime

def fetch_game_results(week_number):
    """
    Fetches actual game results from ESPN or NFL API.
    You'll need to find a suitable API - ESPN's is good.
    """
    # Example: ESPN API (you'll need to implement the actual endpoint)
    url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
    
    response = requests.get(url)
    data = response.json()
    
    # Parse player stats from the response
    # This is pseudo-code - adjust based on actual API structure
    results = []
    for game in data.get('events', []):
        for team in game.get('competitions', [{}])[0].get('competitors', []):
            for player in team.get('statistics', []):
                results.append({
                    'week': week_number,
                    'player_name': player['name'],
                    'stat_type': player['type'],  # e.g., 'passing yards'
                    'actual_value': player['value'],
                    'game_date': game['date']
                })
    
    return results

def save_results(week_number):
    results = fetch_game_results(week_number)
    
    output_dir = f"nfl_data/week_{week_number}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"week_{week_number}_actual_results.csv")
    
    fieldnames = ['week', 'player_name', 'stat_type', 'actual_value', 'game_date']
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"✅ Saved {len(results)} actual results for Week {week_number}")

if __name__ == "__main__":
    week = int(input("Enter week number: "))
    save_results(week)