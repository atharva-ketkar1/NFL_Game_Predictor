import json
import re
import requests
import csv
import os
import time
import random

def fetch_dk_data(session, category_id, subcategory_id):
    """
    Fetches data from the sportscontent API, using the correct URL structure.
    """
    url = (
        f"https://sportsbook-nash.draftkings.com/api/sportscontent/dkusoh/v1/leagues/88808/"
        f"categories/{category_id}/subcategories/{subcategory_id}?"
        f"format=json"
    )
    
    try:
        print(f"  -> Sending request to: .../subcategories/{subcategory_id}")
        # The session now uses the simplified headers for this GET request
        response = session.get(url, timeout=30) # Timeout increased slightly just in case
        response.raise_for_status()
        print("  -> Received response.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for subcategory {subcategory_id}: {e}")
        return None

def parse_player_props(data, week_number, prop_type):
    """
    Parses player props from the new JSON structure by looking in the 'markets' key.
    """
    player_props = []
    
    markets = data.get('markets', [])
    events = data.get('events', [])
    event_map = {e['id']: e for e in events}

    player_name_regex = re.compile(r"^([A-Z][a-z\'\.-]+(?:\s[A-Z][a-z\'\.-]+)*)")

    for market in markets:
        event_id = market.get('eventId')
        event = event_map.get(event_id)
        if not event:
            continue

        market_name = market.get('name', '')
        
        match = player_name_regex.match(market_name)
        if not match:
            continue
            
        player_name = match.group(1).strip()
        
        player_props.append({
            'week': week_number,
            'game': event.get('name'),
            'player_name': player_name,
            'prop_type': prop_type,
            'line': 'N/A',
            'over_odds': 'N/A',
            'under_odds': 'N/A',
            'sportsbook': 'DraftKings'
        })
        
    return player_props

def main():
    try:
        week_number = int(input("Please enter the current NFL week number (e.g., 6): "))
    except ValueError:
        print("Invalid input."); return

    print("Initializing session with simplified headers...")
    session = requests.Session()
    
    # --- Using the simplified headers from your working script ---
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Referer": "https://sportsbook.draftkings.com/",
        "Origin": "https://sportsbook.draftkings.com",
        "Accept": "*/*",
    })
    
    player_props_to_scrape = {
        'Passing Props': (1000, 9524),
        'Rushing Props': (1000, 9526),
        'Receiving Props': (1000, 9525),
    }

    all_player_props = []

    for prop_name, (cat_id, sub_id) in player_props_to_scrape.items():
        print(f"\nFetching DraftKings - {prop_name} (ID: {sub_id})...")
        data = fetch_dk_data(session, cat_id, sub_id)
        if data:
            props = parse_player_props(data, week_number, prop_name)
            all_player_props.extend(props)
            print(f"Found {len(props)} props.")
        time.sleep(random.uniform(2, 4))

    if not all_player_props:
        print("\nNo player props were found. The API structure may have changed.")
        return

    output_dir = 'nfl_data'
    os.makedirs(output_dir, exist_ok=True)
    
    unique_props = [dict(t) for t in {tuple(sorted(d.items())) for d in all_player_props}]
    
    if unique_props:
        props_file = os.path.join(output_dir, f'draftkings_nfl_week_{week_number}_props.csv')
        print(f"\nWriting {len(unique_props)} unique DraftKings props to {props_file}...")
        with open(props_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['week', 'game', 'player_name', 'prop_type', 'line', 'over_odds', 'under_odds', 'sportsbook']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(unique_props)

    print("\nDraftKings scraping complete. ✅")

if __name__ == "__main__":
    main()