# get_data.py (Final Version with Cleanup and Reordering)
import os
from playwright.sync_api import sync_playwright, Page
import pandas as pd
import time
import json
import numpy as np

# ---------- CONFIG ----------
USERNAME = "media"
PASSWORD = "media"
YEARS = range(2019, 2025)

# URLs for fetching stats
GAME_STATS_URL = "https://www.nflgsis.com/GameStatsLive/Statistics/GetTeamStatsByGame?season={season}&seasonType=Reg&clubCode={team}&offense=Y"
SEASON_RANKINGS_URL = "https://www.nflgsis.com/GameStatsLive/Statistics/GetTeamRankings?season={season}&seasonType=Reg&clubCode=NFL&report=EliasRptTeamRankings{report_type}&conference=NFL"
PLUS_MINUS_URL = "https://www.nflgsis.com/GameStatsLive/Statistics/GetPlusMinusStats?season={season}&seasonType=Reg&clubCode={team}&club2=NFL"

if not os.path.exists('data'):
    os.makedirs('data')

def get_dynamic_team_codes(page: Page, year: int) -> list[str]:
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
        if not unique_codes: raise Exception("API responded, but no team codes were found.")
        print(f"✓ Dynamically found {len(unique_codes)} team codes for {year}.")
        return unique_codes
    except Exception as e:
        print(f"❌ Could not find team codes for {year} from API: {e}")
        return []

def fetch_season_stats(page: Page, year: int, teams: list) -> pd.DataFrame:
    print(f"Fetching season-level stats, ranks, and plus/minus data for {year}...")
    try:
        offense_url = SEASON_RANKINGS_URL.format(season=year, report_type="Offense")
        page.goto(offense_url)
        offense_data = json.loads(page.inner_text('pre'))['content']['Content']
        df_off_stats = pd.json_normalize(offense_data['TblStats']).add_prefix('season_off_')
        df_off_ranks = pd.DataFrame.from_dict(offense_data['TblRanks'], orient='index').add_prefix('season_rank_off_')
        
        defense_url = SEASON_RANKINGS_URL.format(season=year, report_type="Defense")
        page.goto(defense_url)
        defense_data = json.loads(page.inner_text('pre'))['content']['Content']
        df_def_stats = pd.json_normalize(defense_data['TblStats']).add_prefix('season_def_')
        df_def_ranks = pd.DataFrame.from_dict(defense_data['TblRanks'], orient='index').add_prefix('season_rank_def_')

        df_off_stats = df_off_stats.rename(columns={'season_off_Club_Code': 'ClubCode'}).set_index('ClubCode')
        df_def_stats = df_def_stats.rename(columns={'season_def_Club_Code': 'ClubCode'}).set_index('ClubCode')
        season_stats_df = df_off_stats.join([df_off_ranks, df_def_stats, df_def_ranks])
        
        plus_minus_list = []
        for team in teams:
            team_plus_minus_url = PLUS_MINUS_URL.format(season=year, team=team)
            page.goto(team_plus_minus_url)
            pm_data = json.loads(page.inner_text('pre'))['content']['Content']
            off_diff = pm_data['Offense']['differences']
            def_diff = pm_data['Defense']['differences']
            flat_data = {'ClubCode': team}
            flat_data.update({f"pm_off_{k}": v for k, v in off_diff.items()})
            flat_data.update({f"pm_def_{k}": v for k, v in def_diff.items()})
            plus_minus_list.append(flat_data)
            
        df_plus_minus = pd.DataFrame(plus_minus_list).set_index('ClubCode')
        final_season_df = season_stats_df.join(df_plus_minus).reset_index()
        print(f"✓ Successfully fetched and combined all season data.")
        return final_season_df
    except Exception as e:
        print(f"❌ Failed to fetch season stats for {year}: {e}")
        return pd.DataFrame()

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
        page.wait_for_selector("a[href='/GameStatsLive/Schedule']", timeout=15000)
        print("✓ Login successful! 🔒")
    except Exception as e:
        page.screenshot(path='login_failure.png')
        print(f"❌ Login failed: {e}")
        browser.close()
        exit()

    for year in YEARS:
        print(f"--- Processing data for {year} season ---")
        TEAMS = get_dynamic_team_codes(page, year)
        if not TEAMS: continue
        
        season_stats_df = fetch_season_stats(page, year, TEAMS)
        if season_stats_df.empty: continue

        all_team_games = []
        for team in TEAMS:
            print(f"Fetching game stats for {team}...")
            url = GAME_STATS_URL.format(season=year, team=team)
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

        if not all_team_games: continue
        df_year = pd.concat(all_team_games, ignore_index=True)
        if 'GameKey' not in df_year.columns: continue
        df_year = df_year.drop_duplicates().copy()
        
        df_year['is_home'] = ~df_year['OpponentClubCode'].str.startswith('@')
        df_year['OpponentClubCode'] = df_year['OpponentClubCode'].str.replace('@', '', regex=False).str.strip()
        
        home_games = df_year[df_year['is_home']].copy()
        away_games = df_year[~df_year['is_home']].copy()

        stat_columns = [col for col in home_games.columns if col not in ['GameKey', 'ClubCode', 'OpponentClubCode', 'is_home']]
        home_games = home_games.rename(columns={col: f'home_{col}' for col in stat_columns})
        away_games = away_games.rename(columns={col: f'away_{col}' for col in stat_columns})

        final_df = pd.merge(home_games, away_games, on='GameKey', suffixes=('_home_perspective', '_away_perspective'))
        
        final_df = final_df.rename(columns={'ClubCode_home_perspective': 'home_team', 'ClubCode_away_perspective': 'away_team'})
        final_df['home_score'] = final_df['home_Points']
        final_df['away_score'] = final_df['away_Points']
        
        final_df = pd.merge(final_df, season_stats_df, left_on='home_team', right_on='ClubCode', how='left')
        final_df = pd.merge(final_df, season_stats_df, left_on='away_team', right_on='ClubCode', how='left', suffixes=('_home_season', '_away_season'))

        final_df['home_team_win'] = (final_df['home_score'] > final_df['away_score']).astype(int)
        final_df['season'] = year
        
        # --- NEW: FINAL CLEANUP AND COLUMN REORDERING ---
        print("Cleaning and reordering final columns...")
        
        # 1. Rename the GameKey column consistently
        final_df = final_df.rename(columns={'GameKey_home_perspective': 'GameKey'})
        
        # 2. Define the desired order
        id_cols = ['GameKey', 'season', 'home_team', 'away_team']
        target_cols = ['home_score', 'away_score', 'home_team_win']
        
        # Dynamically find all the different stat categories
        game_stats_home = sorted([col for col in final_df.columns if col.startswith('home_') and col not in id_cols + target_cols])
        game_stats_away = sorted([col for col in final_df.columns if col.startswith('away_') and col not in id_cols + target_cols])
        season_stats_home = sorted([col for col in final_df.columns if col.endswith('_home_season')])
        season_stats_away = sorted([col for col in final_df.columns if col.endswith('_away_season')])
        
        # 3. Reassemble the DataFrame in a clean order
        final_df = final_df[
            id_cols + 
            target_cols + 
            game_stats_home + 
            game_stats_away + 
            season_stats_home + 
            season_stats_away
        ]
        
        # 4. Remove any accidentally duplicated columns
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        
        output_path = os.path.join('data', f'nfl_raw_data_{year}.csv')
        final_df.to_csv(output_path, index=False)
        print(f"✓ Raw data for {year} saved to {output_path}")
        files_created += 1

    browser.close()

if files_created > 0:
    print(f"\n✅ All raw data has been successfully downloaded. {files_created} file(s) created in the 'data' directory.")
else:
    print("\n❌ Script finished, but no data files were created.")