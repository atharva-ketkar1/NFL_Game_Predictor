import requests
import csv
import os
import time
import random
import re
from collections import defaultdict

# --- CONFIGURATION ---
REGION_CODE = "dkusoh"
GAME_LINES_SUBCATEGORY_ID = 4518  # NFL game lines subcategory

PLAYER_PROP_CATEGORIES = {
    'Passing': 1000,
    'Rushing': 1001,
    'Receiving': 1342,
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
    events = data.get('events', [])
    markets = data.get('markets', [])
    selections = data.get('selections', [])
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
        player_name = re.sub(re.escape(f" {prop_type_name} O/U"), "", market['name'], flags=re.IGNORECASE).strip()
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

def main():
    try:
        week_number = int(input("Enter the current NFL week number (e.g., 6): "))
    except ValueError:
        print("Invalid week number.")
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Referer": "https://sportsbook.draftkings.com/",
        "Origin": "https://sportsbook.draftkings.com",
        "Accept": "*/*",
    })

    output_dir = "nfl_data"
    os.makedirs(output_dir, exist_ok=True)

    # --- 1️⃣ Fetch Game Lines ---
    game_lines_data = fetch_game_lines(session)
    parsed_lines = parse_game_lines(game_lines_data)

    if parsed_lines:
        lines_file = os.path.join(output_dir, f"draftkings_nfl_week_{week_number}_game_lines.csv")
        with open(lines_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = ['game', 'away_team', 'home_team', 'spread', 'spread_odds', 'total_line', 'total_odds', 'moneyline']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parsed_lines)
        print(f"  ✅ Game lines saved to {lines_file}")
    else:
        print("  ⚠️ No game lines parsed.")

    # --- 2️⃣ Fetch Player Props ---
    print("\n--- Starting Player Prop Scraping ---")
    # We only need to hit one category endpoint to get ALL subcategories.
    # Using 'Passing' (1000) as the entry point.
    passing_category_id = PLAYER_PROP_CATEGORIES['Passing']
    all_subs = get_prop_subcategories(session, "Player Props", passing_category_id)

    all_props = []
    for sub in all_subs:
        data = fetch_subcategory_data(session, sub['categoryId'], sub['id'])
        if data:
            props = parse_prop_data(data, sub['name'])
            all_props.extend(props)
            print(f"  -> Found {len(props)} {sub['name']} props")
        time.sleep(random.uniform(1.5, 3.0))

    if all_props:
        props_file = os.path.join(output_dir, f"draftkings_nfl_week_{week_number}_props.csv")
        with open(props_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = ['week', 'game', 'player_name', 'prop_type', 'line', 'over_odds', 'under_odds', 'sportsbook']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for prop in all_props:
                prop['week'] = week_number
            writer.writerows(all_props)
        print(f"  ✅ Player props saved to {props_file}")
    else:
        print("  ⚠️ No props found.")

    print("\n✅ DraftKings scraping complete!")

if __name__ == "__main__":
    main()
