# get_nfl_data.py
# This script dynamically fetches team codes and scrapes a rich set of historical NFL game data.

import os
from playwright.sync_api import sync_playwright, Page
import pandas as pd
import time
import json
import numpy as np # Import numpy for efficient data handling

# ---------- CONFIG ----------
USERNAME = "media"
PASSWORD = "media"
YEARS = range(2019, 2025)

# URL for fetching individual game stats
ENDPOINT_URL = "https://www.nflgsis.com/GameStatsLive/Statistics/GetTeamStatsByGame?season={season}&seasonType=Reg&clubCode={team}&offense=Y"

# Create data directory
if not os.path.exists('data'):
    os.makedirs('data')

def get_dynamic_team_codes(page: Page, year: int) -> list[str]:
    """
    Navigates to the standings API endpoint for a given year and extracts all team codes from the JSON response.
    """
    print(f"Fetching team codes for {year} season from API... 🏈")
    api_url = f"https://nflgsis.com/GameStatsLive/Statistics/GetStandingsAndSeedings?Season={year}"
    
    try:
        page.goto(api_url)
        content = page.inner_text('pre')
        data = json.loads(content)
        
        team_codes = []
        for conference in data['content']['ConferenceStandings']:
            for division in conference['DivisionStandings']:
                for team in division['TeamStandings']:
                    if 'ClubCode' in team and team['ClubCode']:
                        team_codes.append(team['ClubCode'])
        
        unique_codes = sorted(list(set(team_codes)))
        
        if not unique_codes:
            raise Exception("API responded, but no team codes were found.")
            
        print(f"✓ Dynamically found {len(unique_codes)} team codes for {year}.")
        return unique_codes
        
    except Exception as e:
        print(f"❌ Could not find team codes for {year} from API.")
        print(f"Error: {e}")
        return []

# ---------- MAIN SCRIPT with Integrated Scraping ----------
print("Initializing browser and logging in... 🌐")
files_created = 0
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    try:
        page.goto("https://www.nflgsis.com/GameStatsLive/Login")
        page.fill("#Username", USERNAME)
        page.fill("#Password", PASSWORD)
        page.click("#btnLogin")
        
        print("Looking for the 'Accept' button... 🤔")
        try:
            page.get_by_role("button", name="Accept").click(timeout=3000)
            print("✓ 'Accept' button clicked.")
        except Exception:
            print("-> 'Accept' button not found, proceeding...")
            pass
        
        page.wait_for_selector("a[href='/GameStatsLive/Schedule']", timeout=15000)
        print("✓ Login successful! 🔒")

    except Exception as e:
        page.screenshot(path='login_failure.png')
        print(f"❌ Login failed. A screenshot 'login_failure.png' has been saved.")
        print(f"Error details: {e}")
        browser.close()
        exit()

    # Loop through years and fetch data
    for year in YEARS:
        print(f"--- Fetching data for {year} season ---")
        
        TEAMS = get_dynamic_team_codes(page, year)
        if not TEAMS:
            print(f"Could not retrieve teams for {year}. Skipping season.")
            continue

        all_team_games = []
        for team in TEAMS:
            print(f"Fetching data for {team}...")
            url = ENDPOINT_URL.format(season=year, team=team)
            try:
                page.goto(url)
                content = page.inner_text('pre')
                data = json.loads(content)
                
                if "content" in data and "Content" in data["content"] and "Games" in data["content"]["Content"]:
                    df = pd.json_normalize(data["content"]["Content"]["Games"])
                    if not df.empty:
                        df['ClubCode'] = team
                        all_team_games.append(df)

            except Exception:
                pass
            time.sleep(0.5)

        if not all_team_games:
            print(f"No valid game data was found for {year}. Skipping.")
            continue

        df_year = pd.concat(all_team_games, ignore_index=True)
        
        if 'GameKey' not in df_year.columns:
            print(f"Critical column 'GameKey' is missing for season {year}. Skipping.")
            continue

        df_year = df_year.drop_duplicates(subset=['GameKey', 'ClubCode']).copy()
        
        # --- FINAL, FEATURE-RICH DATA PROCESSING ---
        
        # 1. Determine if the game was at home and clean the opponent code
        df_year['is_home'] = ~df_year['OpponentClubCode'].str.startswith('@')
        df_year['OpponentClubCode'] = df_year['OpponentClubCode'].str.replace('@', '', regex=False).str.strip()

        # 2. Separate into home and away dataframes
        home_games = df_year[df_year['is_home']].copy()
        away_games = df_year[~df_year['is_home']].copy()

        # 3. Identify the columns to be prefixed (all except the keys)
        stat_columns = [col for col in home_games.columns if col not in ['GameKey', 'ClubCode', 'OpponentClubCode', 'is_home']]
        
        # 4. Add prefixes to distinguish home and away stats
        home_games = home_games.rename(columns={col: f'home_{col}' for col in stat_columns})
        away_games = away_games.rename(columns={col: f'away_{col}' for col in stat_columns})

        # 5. Merge on GameKey to create one row per game with all stats
        final_df = pd.merge(
            home_games,
            away_games,
            on='GameKey',
            suffixes=('_home', '_away') # Suffixes for any overlapping non-stat columns
        )
        
        # 6. Final cleanup and calculations
        final_df = final_df.rename(columns={'ClubCode_home': 'home_team', 'ClubCode_away': 'away_team'})
        final_df['home_score'] = final_df['home_Points']
        final_df['away_score'] = final_df['away_Points']
        final_df['home_team_win'] = (final_df['home_score'] > final_df['away_score']).astype(int)
        final_df['season'] = year
        
        # Select and reorder columns for a clean final output
        # You can customize this list to keep the stats you care about
        core_cols = ['GameKey', 'season', 'home_team', 'away_team', 'home_score', 'away_score', 'home_team_win']
        home_stat_cols = [col for col in final_df.columns if col.startswith('home_') and 'team' not in col and 'score' not in col]
        away_stat_cols = [col for col in final_df.columns if col.startswith('away_') and 'team' not in col and 'score' not in col]
        
        final_df = final_df[core_cols + sorted(home_stat_cols) + sorted(away_stat_cols)]
        
        output_path = os.path.join('data', f'nfl_game_data_{year}.csv')
        final_df.to_csv(output_path, index=False)
        print(f"✓ Data for {year} saved to {output_path}")
        files_created += 1

    browser.close()

if files_created > 0:
    print(f"\n✅ All data has been successfully downloaded. {files_created} file(s) created in the 'data' directory.")
else:
    print("\n❌ Script finished, but no data files were created. Please check for errors in the output above.")