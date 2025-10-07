import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- CONFIGURATION ---
API_KEY = 'ed63bd22ecf8ad019b69608e25b5c9c3'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'
MARKETS = 'h2h,spreads,totals'

# --- DATE RANGE SELECTION ---
START_DATE = '2024-09-05'
END_DATE = '2024-10-28'

# --- FILE PATHS ---
GAMES_FILE = 'games.csv'
TEAMS_FILE = 'teams.csv'
OUTPUT_CSV_FILE = 'analyzed_odds_data.csv'


def main():
    """
    Loads game and team data, fetches closing line odds, merges the data,
    and saves a comprehensive record to a CSV file.
    """
    print(f"🚀 Starting analysis for date range: {START_DATE} to {END_DATE}...")

    # --- Step 1: Load and Merge Local Data ---
    try:
        games_df = pd.read_csv(GAMES_FILE)
        teams_df = pd.read_csv(TEAMS_FILE)
    except FileNotFoundError as e:
        print(f"❌ ERROR: Make sure '{e.filename}' is in the same folder as the script.")
        return

    # Prepare team names for merging using the correct column names
    teams_map = teams_df[['team', 'full']]
    
    games_df = pd.merge(
        games_df,
        teams_map.rename(columns={'team': 'home_team', 'full': 'home_team_name'}),
        on='home_team',
        how='left'
    )
    games_df = pd.merge(
        games_df,
        teams_map.rename(columns={'team': 'away_team', 'full': 'away_team_name'}),
        on='away_team',
        how='left'
    )

    # Filter for the selected date range and played games
    mask = (
        (games_df['gameday'] >= START_DATE) &
        (games_df['gameday'] <= END_DATE) &
        (games_df['game_type'].isin(['REG', 'WC', 'DIV', 'CON', 'SB'])) &
        (games_df['result'].notna())
    )
    games_to_process = games_df.loc[mask].copy()

    # FIX: Remove duplicate games to prevent the infinite loop
    games_to_process.drop_duplicates(subset='game_id', keep='first', inplace=True)

    print(f"✅ Loaded and filtered {len(games_to_process)} unique completed games.")

    if games_to_process.empty:
        print("No games found in the specified date range. Exiting.")
        return

    all_game_lines = []

    # --- Step 2: Fetch Closing Odds for Each Game ---
    for index, game in games_to_process.iterrows():
        game_date = game['gameday']
        game_time_str = game['gametime']
        
        try:
            dt_obj = datetime.strptime(f"{game_date} {game_time_str}", "%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            print(f"  Skipping game due to invalid time format: {game_time_str}")
            continue

        closing_time_obj = dt_obj - timedelta(minutes=5)
        iso_date = closing_time_obj.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"\nFetching closing odds for {game['away_team']} @ {game['home_team']} on {game_date}...")

        try:
            url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds-history/?apiKey={API_KEY}&regions={REGIONS}&markets={MARKETS}&date={iso_date}"
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()

            odds_data = []
            if isinstance(response_json, dict) and 'data' in response_json:
                odds_data = response_json['data']
            
            if not odds_data:
                print("  No odds data returned from API for this time.")
                continue

            api_game = next((g for g in odds_data if g['home_team'] == game['home_team_name'] and g['away_team'] == game['away_team_name']), None)

            if not api_game:
                print(f"  Could not find matching game in API response for {game['home_team_name']}.")
                continue

            print(f"  Found matching game. Processing {len(api_game.get('bookmakers', []))} bookmakers.")

            for bookmaker in api_game.get('bookmakers', []):
                markets = {m['key']: m for m in bookmaker.get('markets', [])}
                h2h = markets.get('h2h', {})
                spreads = markets.get('spreads', {})
                totals = markets.get('totals', {})
                
                home_team_name_full = game['home_team_name']
                away_team_name_full = game['away_team_name']
                
                h2h_home = next((o for o in h2h.get('outcomes', []) if o.get('name') == home_team_name_full), {})
                h2h_away = next((o for o in h2h.get('outcomes', []) if o.get('name') == away_team_name_full), {})
                spread_home = next((o for o in spreads.get('outcomes', []) if o.get('name') == home_team_name_full), {})
                spread_away = next((o for o in spreads.get('outcomes', []) if o.get('name') == away_team_name_full), {})
                total_over = next((o for o in totals.get('outcomes', []) if o.get('name') == 'Over'), {})
                total_under = next((o for o in totals.get('outcomes', []) if o.get('name') == 'Under'), {})

                all_game_lines.append({
                    'game_id': game['game_id'], 'gameday': game['gameday'],
                    'home_team': game['home_team'], 'away_team': game['away_team'],
                    'bookmaker': bookmaker['title'], 'h2h_home_price': h2h_home.get('price'),
                    'h2h_away_price': h2h_away.get('price'), 'spread_home_point': spread_home.get('point'),
                    'spread_home_price': spread_home.get('price'), 'spread_away_point': spread_away.get('point'),
                    'spread_away_price': spread_away.get('price'), 'total_over_point': total_over.get('point'),
                    'total_over_price': total_over.get('price'), 'total_under_point': total_under.get('point'),
                    'total_under_price': total_under.get('price'), 'home_score': game['home_score'],
                    'away_score': game['away_score'],
                })
            
            time.sleep(1) 

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching API data: {e}")
            time.sleep(1)
            continue
    
    if not all_game_lines:
        print("\nNo data was collected.")
        return

    df_lines = pd.DataFrame(all_game_lines)
    
    try:
        df_lines.to_csv(OUTPUT_CSV_FILE, mode='a', header=not os.path.exists(OUTPUT_CSV_FILE), index=False)
        print(f"\n✅ Successfully saved/appended {len(df_lines)} rows to {OUTPUT_CSV_FILE}")
    except IOError as e:
        print(f"\n❌ Error saving data to CSV: {e}")

if __name__ == '__main__':
    main()