import requests
import csv
import os
import time
import random
import re
from collections import defaultdict
from datetime import datetime

# --- CONFIGURATION ---
REGION_CODE = "dkusoh"
GAME_LINES_SUBCATEGORY_ID = 4518  # NFL game lines subcategory

PLAYER_PROP_CATEGORIES = {
    'Passing': 1000,
    'Rushing': 1001,
    'Receiving': 1342,
}

# <<< NEW: Configuration for "Longest" props with their specific subcategory IDs
LONGEST_PROP_SUBCATEGORIES = {
    'Longest Rush': '14880',
    'Longest Reception': '14881',
    'Longest Passing Completion': '9526'
}

# ============================================================
# 🏈 GAME LINES FETCH + PARSE (Merged from your working snippet)
# ============================================================

def fetch_game_lines(session):
    """
    Fetches all game line data (spreads, totals, moneylines)
    from the new working DraftKings API endpoint.
    """
    url = (
        "https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
        "?isBatchable=false&templateVars=88808%2C4518"
        "&eventsQuery=%24filter%3DleagueId%20eq%20%2788808%27%20AND%20clientMetadata%2FSubcategories%2Fany%28s%3A%20s%2FId%20eq%20%274518%27%29"
        "&marketsQuery=%24filter%3DclientMetadata%2FsubCategoryId%20eq%20%274518%27%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29"
        "&include=Events&entity=events"
    )

    print("\nFetching NFL game lines from DraftKings...")
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("  ✅ Game lines data received successfully.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error fetching game lines: {e}")
        return None


def parse_game_lines(data):
    """
    Parses the DraftKings NFL game lines response and extracts:
    - Spread
    - Total (Over/Under)
    - Moneyline
    """
    if not data:
        return []

    events = data.get("events", [])
    markets = data.get("markets", [])
    selections = data.get("selections", [])

    selections_by_market_id = defaultdict(list)
    for sel in selections:
        selections_by_market_id[sel['marketId']].append(sel)

    markets_by_event_id = defaultdict(list)
    for market in markets:
        markets_by_event_id[market['eventId']].append(market)

    all_game_lines = []

    for event in events:
        participants = event.get("participants", [])
        if len(participants) != 2:
            continue

        home = next((p for p in participants if p.get("venueRole") == "Home"), participants[0])
        away = next((p for p in participants if p.get("venueRole") == "Away"), participants[1])
        game_markets = markets_by_event_id.get(event['id'], [])
        if not game_markets:
            continue

        game_info = {
            'game': f"{away['name']} @ {home['name']}",
            'away_team': away['name'],
            'home_team': home['name'],
            'spread': None,
            'spread_odds': None,
            'total_line': None,
            'total_odds': None,
            'moneyline': None
        }

        for market in game_markets:
            market_selections = selections_by_market_id.get(market['id'], [])
            if not market_selections:
                continue

            first_sel = market_selections[0]

            # Spread
            if first_sel.get('points') is not None and first_sel.get('label', '').lower() not in ['over', 'under']:
                try:
                    odds_val = first_sel['displayOdds']['american'].replace('\u2212', '-')
                    game_info['spread'] = first_sel.get('points')
                    game_info['spread_odds'] = odds_val
                except Exception:
                    pass

            # Total (Over/Under)
            elif first_sel.get('points') is not None and first_sel.get('label', '').lower() == 'over':
                try:
                    over_odds = first_sel['displayOdds']['american'].replace('\u2212', '-')
                    game_info['total_line'] = first_sel.get('points')
                    game_info['total_odds'] = over_odds
                except Exception:
                    pass

            # Moneyline
            elif first_sel.get('points') is None:
                try:
                    away_odds = market_selections[0]['displayOdds']['american'].replace('\u2212', '-')
                    home_odds = market_selections[1]['displayOdds']['american'].replace('\u2212', '-')
                    game_info['moneyline'] = f"{market_selections[0]['label']}: {away_odds} / {market_selections[1]['label']}: {home_odds}"
                except Exception:
                    pass

        all_game_lines.append(game_info)

    print(f"  ✅ Parsed {len(all_game_lines)} NFL game lines successfully.")
    return all_game_lines


# ============================================================
# 🧠 PLAYER PROPS FUNCTIONS (unchanged from your main script)
# ============================================================

# <<< NEW: Generic function to fetch props from the direct market-style endpoints
def fetch_direct_prop_data(session, subcategory_id, prop_name):
    """
    Fetches prop data from a direct subcategory endpoint that returns
    events, markets, and selections directly.
    """
    url = (
        f"https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
        f"?isBatchable=false&templateVars=88808%2C{subcategory_id}"
        f"&eventsQuery=%24filter%3DleagueId%20eq%20%2788808%27%20AND%20clientMetadata%2FSubcategories%2Fany%28s%3A%20s%2FId%20eq%20%27{subcategory_id}%27%29"
        f"&marketsQuery=%24filter%3DclientMetadata%2FsubCategoryId%20eq%20%27{subcategory_id}%27%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29"
        f"&include=Events&entity=events"
    )
    print(f"Fetching '{prop_name}' props from DraftKings...")
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"  ✅ '{prop_name}' data received successfully.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error fetching '{prop_name}': {e}")
        return None

def find_subcategories_in_response(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                if 'id' in value[0] and 'categoryId' in value[0] and 'name' in value[0]:
                    return value
            found = find_subcategories_in_response(value)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_subcategories_in_response(item)
            if found:
                return found
    return []

def get_prop_subcategories(session, category_name, category_id):
    url = f"https://sportsbook-nash.draftkings.com/api/sportscontent/{REGION_CODE}/v1/leagues/88808/categories/{category_id}?format=json"
    print(f"\nDiscovering subcategories for '{category_name}'...")
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        subcategories = find_subcategories_in_response(data)
        return [
            {'id': sub['id'], 'name': sub['name'].replace(' O/U', ''), 'categoryId': sub['categoryId']}
            for sub in subcategories if 'O/U' in sub.get('name', '')
        ]
    except requests.exceptions.RequestException:
        return []

def fetch_subcategory_data(session, category_id, sub_id):
    url = f"https://sportsbook-nash.draftkings.com/api/sportscontent/{REGION_CODE}/v1/leagues/88808/categories/{category_id}/subcategories/{sub_id}?format=json"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None

def parse_prop_data(data, prop_type_name):
    if not data:
        return []
    # <<< MODIFIED: Handle both API response structures gracefully
    events = data.get('events', [])
    markets = data.get('markets', [])
    selections = data.get('selections', [])

    # If the primary keys are not found, check for a nested structure
    if not events and 'eventGroup' in data and data['eventGroup'].get('events'):
         events = data['eventGroup']['events']
         markets = data['eventGroup']['events'][0].get('markets', [])
         selections = data['eventGroup']['events'][0]['markets'][0].get('outcomes', [])


    event_map = {event['id']: event['name'] for event in events}
    selections_by_market = defaultdict(dict)
    
    for sel in selections:
        market_id = sel.get('marketId')
        label = sel.get('label', '').lower()
        if market_id and label in ['over', 'under']:
            selections_by_market[market_id][label] = sel

    parsed_props = []
    for market in markets:
        market_id = market.get('id')
        outcomes = selections_by_market.get(market_id)
        if not outcomes or 'over' not in outcomes or 'under' not in outcomes:
            continue
        over_sel = outcomes['over']
        under_sel = outcomes['under']
        
        # NEW, ROBUST LOGIC:
        # This reliably finds the prop type in the market name and extracts the player name before it.
        # It handles variations like "Rec Yards" vs "Receiving Yards" gracefully.
        search_name = prop_type_name.split(' ')[0] # e.g., 'Receiving' or 'Rec' or 'Passing'
        market_name = market['name']
        
        # Find the position of the prop type in the market string
        match = re.search(r'\b' + re.escape(search_name), market_name, re.IGNORECASE)
        if match:
            # If a match is found, the player's name is everything before it
            player_name = market_name[:match.start()].strip()
        else:
            # Fallback for safety, in case the pattern is unexpected
            player_name = market_name.replace(f" {prop_type_name} O/U", "").strip()

        parsed_props.append({
            'player_name': player_name,
            'game': event_map.get(market.get('eventId')),
            'prop_type': prop_type_name,
            'line': over_sel.get('points'),
            'over_odds': over_sel.get('displayOdds', {}).get('american'),
            'under_odds': under_sel.get('displayOdds', {}).get('american'),
            'sportsbook': 'DraftKings'
        })
    return parsed_props

# ============================================================
# 🚀 MAIN EXECUTION
# ============================================================

def run_scraper(week_number):
    # This function now accepts 'week_number' as an argument
    # The input() call has been removed

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Referer": "https://sportsbook.draftkings.com/",
        "Origin": "https://sportsbook.draftkings.com",
        "Accept": "*/*",
    })

    # --- Create week-specific output folder ---
    base_dir = "nfl_data"
    week_dir = os.path.join(base_dir, f"week_{week_number}")
    os.makedirs(week_dir, exist_ok=True)

    # --- 1️⃣ Fetch Game Lines ---
    game_lines_data = fetch_game_lines(session)
    parsed_lines = parse_game_lines(game_lines_data)

    # We will save lines at the end, along with props

    # --- 2️⃣ Fetch Player Props ---
    print("\n--- Starting Player Prop Scraping ---")
    all_props = []

    passing_category_id = PLAYER_PROP_CATEGORIES['Passing']
    all_subs = get_prop_subcategories(session, "Player Props", passing_category_id)

    for sub in all_subs:
        data = fetch_subcategory_data(session, sub['categoryId'], sub['id'])
        if data:
            props = parse_prop_data(data, sub['name'])
            all_props.extend(props)
            print(f"  -> Found {len(props)} {sub['name']} props")
        time.sleep(random.uniform(1.5, 3.0))

    print("\n--- Fetching 'Longest' Player Props ---")
    for prop_name, sub_id in LONGEST_PROP_SUBCATEGORIES.items():
        data = fetch_direct_prop_data(session, sub_id, prop_name)
        if data:
            props = parse_prop_data(data, prop_name)
            all_props.extend(props)
            print(f"  -> Found {len(props)} {prop_name} props")
        time.sleep(random.uniform(1.5, 3.0))

    # --- MODIFIED: Add timestamp to all new data before saving ---
    scrape_time = datetime.now().isoformat()
    for prop in all_props:
        prop['week'] = week_number
        prop['scrape_timestamp'] = scrape_time
        
    for line in parsed_lines:
        line['scrape_timestamp'] = scrape_time

    # --- 3️⃣ Save All Props and Lines to CSV ---
    
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
    if all_props:
        props_file = os.path.join(week_dir, f"draftkings_nfl_week_{week_number}_props_history.csv")
        props_fieldnames = ['week', 'game', 'player_name', 'prop_type', 'line', 'over_odds', 'under_odds', 'sportsbook']
        append_to_historical_csv(all_props, props_file, props_fieldnames)
    else:
        print("\n  ⚠️ No new player props found.")

    # --- MODIFIED: 2. Append Game Lines ---
    if parsed_lines:
        lines_file = os.path.join(week_dir, f"draftkings_nfl_week_{week_number}_game_lines_history.csv")
        lines_fieldnames = ['game', 'away_team', 'home_team', 'spread', 'spread_odds', 'total_line', 'total_odds', 'moneyline']
        append_to_historical_csv(parsed_lines, lines_file, lines_fieldnames)
    else:
        print("\n  ⚠️ No new game lines found.")

    print("\n✅ DraftKings scraping complete!")

if __name__ == "__main__":
    # This block now runs ONLY if you run this file directly
    try:
        week_num = int(input("Enter the current NFL week number (e.g., 6): "))
        run_scraper(week_num) # Call the refactored function
    except ValueError:
        print("Invalid input. Please enter a whole number.")