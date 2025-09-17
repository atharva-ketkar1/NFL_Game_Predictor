import requests
import json

url = "https://api.sportsbook.fanduel.com/sbapi/content-managed-page?page=CUSTOM&customPageId=nfl&pbHorizontal=false&_ak=FhMFpcPWXMeyZxOx&timezone=America%2FNew_York"
headers = {
    'x-sportsbook-region': 'OH',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'Referer': 'https://sportsbook.fanduel.com/'
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # All the data we need is nested in these 'attachments'
    attachments = data.get('attachments', {})
    events = attachments.get('events', {})
    markets = attachments.get('markets', {})

    # --- The Correct Logic ---
    # 1. Find the main "All NFL games" coupon component which holds the game list.
    all_games_coupon = None
    for coupon_id, coupon_data in data.get('layout', {}).get('coupons', {}).items():
        if coupon_data.get('title') == 'All NFL games':
            all_games_coupon = coupon_data
            break

    if not all_games_coupon:
        print("Could not find the 'All NFL games' coupon. The API structure may have changed.")
    else:
        # 2. Get the list of game rows from this coupon. Each row has an eventId and marketIds.
        game_rows = all_games_coupon.get('display', [{}])[0].get('rows', [])

        # 3. Loop through the game rows to get the odds
        for row in game_rows:
            event_id = row.get('eventId')
            market_ids = row.get('marketIds', [])
            
            event_data = events.get(str(event_id))
            if not event_data or ' @ ' not in event_data.get('name', ''):
                continue

            # Print Game Info
            team1 = event_data['name'].split(' @ ')[0]
            team2 = event_data['name'].split(' @ ')[1]
            print(f"--- Game: {team1} @ {team2} ---")

            # Print Odds using the market IDs provided in the row
            for market_id in market_ids:
                market = markets.get(str(market_id))
                if market:
                    # Use the actual market name from the data
                    market_name = market.get('marketName')
                    if market_name == 'Total Match Points':
                        market_name = 'Total' # Shorten for readability
                    elif market_name == 'Spread':
                        market_name = 'Point Spread'

                    print(f"  Market: {market_name}")
                    
                    for runner in market.get('runners', []):
                        runner_name = runner.get('runnerName')
                        odds = runner.get('winRunnerOdds', {}).get('americanDisplayOdds', {}).get('americanOdds')
                        handicap = runner.get('handicap')

                        if market_name == 'Point Spread':
                            handicap_str = f"({handicap:+.1f})" if handicap is not None else ""
                            print(f"    - {runner_name} {handicap_str}: {odds}")
                        elif market_name == 'Total':
                            print(f"    - {runner_name} ({handicap:.1f}): {odds}")
                        else: # Moneyline
                            print(f"    - {runner_name}: {odds}")
            print("\n")

except Exception as e:
    print(f"An unexpected error occurred: {e}")