import requests
import json
from collections import defaultdict

# The URL you found and confirmed is working
url = "https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets?isBatchable=false&templateVars=88808%2C4518&eventsQuery=%24filter%3DleagueId%20eq%20%2788808%27%20AND%20clientMetadata%2FSubcategories%2Fany%28s%3A%20s%2FId%20eq%20%274518%27%29&marketsQuery=%24filter%3DclientMetadata%2FsubCategoryId%20eq%20%274518%27%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29&include=Events&entity=events"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    events = data.get("events", [])
    markets = data.get("markets", [])
    selections = data.get("selections", [])

    selections_by_market_id = defaultdict(list)
    for selection in selections:
        selections_by_market_id[selection['marketId']].append(selection)

    markets_by_event_id = defaultdict(list)
    for market in markets:
        markets_by_event_id[market['eventId']].append(market)

    # --- New, Cleaner Printing Logic ---
    for event in events:
        participants = event.get("participants", [])
        if len(participants) != 2:
            continue

        home = next((p for p in participants if p.get("venueRole") == "Home"), participants[0])
        away = next((p for p in participants if p.get("venueRole") == "Away"), participants[1])
        
        game_markets = markets_by_event_id.get(event['id'], [])
        if not game_markets:
            continue

        # Variables to store the odds for a clean, single-line output
        spread_str = "N/A"
        total_str = "N/A"
        moneyline_str = "N/A"

        for market in game_markets:
            market_selections = selections_by_market_id.get(market['id'], [])
            if not market_selections:
                continue

            first_selection = market_selections[0]
            
            # Identify Point Spread
            if first_selection.get('points') is not None and first_selection.get('label', '').lower() not in ['over', 'under']:
                team1 = market_selections[0]
                team2 = market_selections[1]
                
                # FIX: Perform the .replace() before the f-string
                odds_val = team1['displayOdds']['american'].replace('\u2212', '-')
                spread_str = f"{team1['label']} ({team1.get('points', 0):+.1f}) {odds_val}"

            # Identify Total (Over/Under)
            elif first_selection.get('points') is not None and first_selection.get('label', '').lower() == 'over':
                over_selection = first_selection

                # FIX: Perform the .replace() before the f-string
                over_odds = over_selection['displayOdds']['american'].replace('\u2212', '-')
                total_str = f"O/U {over_selection.get('points', 0):.1f} ({over_odds})"

            # Identify Moneyline (Your existing logic here was already correct!)
            elif first_selection.get('points') is None:
                team1_odds = market_selections[0]['displayOdds']['american'].replace('\u2212', '-')
                team2_odds = market_selections[1]['displayOdds']['american'].replace('\u2212', '-')
                moneyline_str = f"{market_selections[0]['label']}: {team1_odds} / {market_selections[1]['label']}: {team2_odds}"

        # Print the consolidated, single-line summary for the game
        print(f"• {away['name']} @ {home['name']} | Spread: {spread_str} | Total: {total_str} | Moneyline: {moneyline_str}")


except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")