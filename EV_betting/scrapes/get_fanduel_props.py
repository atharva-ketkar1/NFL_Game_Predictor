import requests
import json
import time
import csv
import os
from datetime import datetime, timedelta, timezone

def get_nfl_main_page_data():
    """Fetches the main NFL page and returns the raw data needed for parsing."""
    
    # --- MODIFIED: Added cache-buster ---
    cache_buster = int(time.time())
    url = f"https://api.sportsbook.fanduel.com/sbapi/content-managed-page?page=CUSTOM&customPageId=nfl&pbHorizontal=false&_ak=FhMFpcPWXMeyZxOx&timezone=America%2FNew_York&_={cache_buster}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'x-sportsbook-region': 'OH',
        'Cache-Control': 'no-cache, no-store, must-revalidate',  # Add this
        'Pragma': 'no-cache',  # Add this
        'Expires': '0'  # Add this
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NFL main page: {e}")
        return None


def get_player_props(event_id, prop_tab):
    """Fetches the player props for a specific game (event_id) and prop tab."""
    cache_buster = int(time.time())
    url = f"https://api.sportsbook.fanduel.com/sbapi/event-page?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab={prop_tab}&_={cache_buster}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'x-sportsbook-region': 'OH',
        'Cache-Control': 'no-cache, no-store, must-revalidate',  # Add this
        'Pragma': 'no-cache',  # Add this
        'Expires': '0'  # Add this
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None


def extract_team_name_from_logo(logo_url):
    """Extracts and formats a team name from a FanDuel logo URL."""
    if not logo_url:
        return "Unknown Team"
    try:
        team_slug = logo_url.split('/')[-1].replace('.png', '').replace('_jersey', '')
        return ' '.join(word.capitalize() for word in team_slug.split('_'))
    except Exception:
        return "Unknown Team"


def get_upcoming_nfl_games(main_page_data):
    """Parses the main page data to find games scheduled within the next 8 days."""
    attachments = main_page_data.get('attachments', {})
    events_data = attachments.get('events', {})

    all_games_coupon = None
    for coupon_data in main_page_data.get('layout', {}).get('coupons', {}).values():
        if coupon_data.get('title') == 'All NFL games':
            all_games_coupon = coupon_data
            break

    if not all_games_coupon:
        print("Could not find the 'All NFL games' coupon in the API response.")
        return []

    game_rows = all_games_coupon.get('display', [{}])[0].get('rows', [])
    now_utc = datetime.now(timezone.utc)
    upcoming_events = []

    for row in game_rows:
        event_id = row.get('eventId')
        if not event_id:
            continue
        event_detail = events_data.get(str(event_id))
        if not event_detail:
            continue

        open_time_str = event_detail.get('openTime')
        if open_time_str:
            if open_time_str.endswith('Z'):
                open_time_str = open_time_str[:-1] + '+00:00'
            try:
                open_time = datetime.fromisoformat(open_time_str)
                if open_time - now_utc < timedelta(days=8):
                    upcoming_events.append((event_detail, row.get('marketIds', [])))
            except ValueError:
                upcoming_events.append((event_detail, row.get('marketIds', [])))
        else:
            upcoming_events.append((event_detail, row.get('marketIds', [])))  # Failsafe

    return upcoming_events


def run_scraper(week_number):
    # This function now accepts 'week_number' as an argument
    # The input() call has been removed

    print("\nFetching all upcoming NFL games...")
    main_page_data = get_nfl_main_page_data()
    if not main_page_data:
        print("Could not fetch main page data. Exiting.")
        return

    upcoming_events = get_upcoming_nfl_games(main_page_data)
    if not upcoming_events:
        print("Found 0 games scheduled for the upcoming week.")
        return
    else:
        print(f"Found {len(upcoming_events)} games scheduled for the upcoming week.")

    markets_data = main_page_data.get('attachments', {}).get('markets', {})
    all_props_data, all_game_lines_data = [], []

    for event, market_ids in upcoming_events:
        event_id, game_name = event['eventId'], event['name']
        print(f"\n--- Scraping Game: {game_name} ---")

        # Scrape Game Lines
        game_line = {'week': week_number, 'game': game_name}
        if ' @ ' in game_name:
            game_line.update({
                'away_team': game_name.split(' @ ')[0],
                'home_team': game_name.split(' @ ')[1]
            })

        for market_id in market_ids:
            market = markets_data.get(str(market_id))
            if not market or len(market.get('runners', [])) != 2:
                continue

            market_name = market.get('marketName')
            runners = market.get('runners', [])
            if market_name == 'Spread':
                game_line.update({
                    'away_spread_line': runners[0].get('handicap'),
                    'away_spread_odds': runners[0].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds'),
                    'home_spread_line': runners[1].get('handicap'),
                    'home_spread_odds': runners[1].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds')
                })
            elif market_name == 'Moneyline':
                game_line.update({
                    'away_moneyline': runners[0].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds'),
                    'home_moneyline': runners[1].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds')
                })
            elif market_name == 'Total Match Points':
                game_line.update({
                    'total_line': runners[0].get('handicac'),
                    'over_odds': runners[0].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds'),
                    'under_odds': runners[1].get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds')
                })
        all_game_lines_data.append(game_line)

        # Scrape Player Props
        for tab_key in ["passing-props", "receiving-props", "rushing-props"]:
            prop_data = get_player_props(event_id, tab_key)
            if not prop_data or 'attachments' not in prop_data or 'markets' not in prop_data['attachments']:
                continue

            for market in prop_data['attachments']['markets'].values():
                if " - " not in market.get('marketName', ''):
                    continue
                player_name, prop_type = market['marketName'].rsplit(' - ', 1)
                runners = market.get('runners', [])
                if len(runners) != 2:
                    continue

                over_runner = next((r for r in runners if r.get('result', {}).get('type') == 'OVER'), None)
                under_runner = next((r for r in runners if r.get('result', {}).get('type') == 'UNDER'), None)
                if not over_runner or not under_runner:
                    continue

                logo_url = over_runner.get('secondaryLogo', '')
                all_props_data.append({
                    'week': week_number,
                    'game': game_name,
                    'player_name': player_name,
                    'team_name': extract_team_name_from_logo(logo_url),
                    'team_logo': logo_url,
                    'prop_type': prop_type,
                    'line': over_runner.get('handicap'),
                    'over_odds': over_runner.get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds') if over_runner else None,
                    'under_odds': under_runner.get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds') if under_runner else None,
                    'sportsbook': 'FanDuel'
                })
            time.sleep(0.5)

    # --- MODIFIED: Add timestamp to all new data before saving ---
    scrape_time = datetime.now().isoformat()
    for prop in all_props_data:
        prop['scrape_timestamp'] = scrape_time
    for line in all_game_lines_data:
        line['scrape_timestamp'] = scrape_time

    # --- MODIFIED: Write to week_{week_number} subfolder ---
    base_dir = "nfl_data"
    week_dir = os.path.join(base_dir, f"week_{week_number}")
    os.makedirs(week_dir, exist_ok=True)

    # --- MODIFIED: Helper function for appending ---
    def append_to_historical_csv(new_data, output_file, default_fieldnames):
        """Appends new data to a CSV, writing a header if the file is new."""
        if not new_data:
            print(f"No new data to write for {output_file}.")
            return
            
        # Add timestamp to the fieldnames
        fieldnames = default_fieldnames + ['scrape_timestamp']
        file_exists = os.path.exists(output_file)
        
        try:
            with open(output_file, "a", newline="", encoding="utf-8") as f:
                # Use extrasaction='ignore' to be safe with any column mismatches
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                if not file_exists:
                    writer.writeheader()  # Write header only if file is new
                writer.writerows(new_data)
            print(f"  Appended {len(new_data)} new rows to {output_file}")
        except Exception as e:
             print(f"  ERROR writing file {output_file}: {e}")

    # --- MODIFIED: 1. Append Player Props ---
    if all_props_data:
        props_file = os.path.join(week_dir, f"fanduel_nfl_week_{week_number}_props_history.csv")
        # Define fieldnames *without* timestamp (it's added by the helper)
        props_fieldnames = ['week', 'game', 'player_name', 'team_name', 'team_logo', 
                            'prop_type', 'line', 'over_odds', 'under_odds', 'sportsbook']
        append_to_historical_csv(all_props_data, props_file, props_fieldnames)
    else:
        print("\nNo new player props found.")

    # --- MODIFIED: 2. Append Game Lines ---
    if all_game_lines_data:
        lines_file = os.path.join(week_dir, f"fanduel_nfl_week_{week_number}_game_lines_history.csv")
        lines_fieldnames = ['week', 'game', 'away_team', 'home_team', 'away_spread_line', 
                            'away_spread_odds', 'home_spread_line', 'home_spread_odds', 
                            'away_moneyline', 'home_moneyline', 'total_line', 
                            'over_odds', 'under_odds']
        append_to_historical_csv(all_game_lines_data, lines_file, lines_fieldnames)
    else:
        print("\nNo new game lines found.")

    print("\n✅ Scraping complete! Files saved in:", week_dir)


if __name__ == "__main__":
    # This block now runs ONLY if you run this file directly
    try:
        week_num = int(input("Please enter the current NFL week number (e.g., 6): "))
        run_scraper(week_num) # Call the refactored function
    except ValueError:
        print("Invalid input. Please enter a whole number.")