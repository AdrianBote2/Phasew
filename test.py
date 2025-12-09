import database_functions as db
import sqlite3

def run_comprehensive_test():
    database = "nfl_stats.sqlite"
    conn = db.openConnection(database)

    if not conn:
        print("Failed to connect to database. Exiting tests.")
        return

    print("\n==========================================")
    print("STARTING COMPREHENSIVE DATABASE TEST")
    print("==========================================\n")

    # ---------------------------------------------------------
    # 1. TEST DATA CREATION (Insertions)
    # ---------------------------------------------------------
    print("--- 1. Testing INSERT Functions ---")

    # Test Variables
    test_player_id = "TEST_001"
    test_player_name = "Testy McTesterson"
    test_team = "SF" # Assuming 'SF' exists in your teams table
    test_season = 2025
    test_game_id = "2025_01_SF_TEST"
    test_coach_id = 9999
    test_coach_name = "Coach Beard"

    # A. Add Player
    print(f"[*] Adding Player: {test_player_name}...")
    db.addPlayer(conn, 
        player_id=test_player_id, 
        player_name=test_player_name, 
        team=test_team, 
        birth_year=2000, 
        draft_year=2022, 
        draft_ovr=1, 
        height=72, 
        weight=200, 
        position="QB", 
        season_year=test_season, 
        week=1
    )

    # B. Add Player Game Stats
    print(f"[*] Adding Game Stats for: {test_player_name}...")
    db.addPlayerGameStats(conn, 
        season=test_season, 
        player_id=test_player_id, 
        player_name=test_player_name, 
        week=1, 
        team=test_team, 
        passing_yards=350.5, 
        pass_touchdown=3, 
        rushing_yards=20.0,
        interception=1
    )

    # C. Add Coach
    print(f"[*] Adding Coach: {test_coach_name}...")
    db.addCoach(conn, 
        coach_name=test_coach_name, 
        coach_id=test_coach_id, 
        team=test_team, 
        hire_year=test_season
    )

    # D. Add Game
    print(f"[*] Adding Game: {test_game_id}...")
    db.addGame(conn, 
        game_id=test_game_id, 
        season=test_season, 
        week=1, 
        season_type="REG", 
        away_team="KC",  # Assuming KC exists
        home_team=test_team, 
        home_win=1
    )

    # ---------------------------------------------------------
    # 2. TEST DATA UPDATES
    # ---------------------------------------------------------
    print("\n--- 2. Testing UPDATE Functions ---")

    # A. Update Player Info
    print(f"[*] Updating Player Info (Name, Weight, Position)...")
    db.updatePlayerName(conn, test_player_id, "Testy McTesterson II")
    db.updatePlayerWeight(conn, test_player_id, 215)
    db.updatePlayerPosition(conn, test_player_id, "TE") # Changed from QB to TE

    # B. Update Player Team
    print(f"[*] Updating Player Team to 'KC'...")
    db.updatePlayerTeam(conn, test_player_id, "KC", test_season, week=2)

    # C. Update Team Info (and revert it instantly so we don't mess up real data)
    print(f"[*] Testing Team Update (City/Name)...")
    db.updateTeamCity(conn, "SF", "San Francisco (Test)")
    db.updateTeamName(conn, "SF", "49ers (Test)")
    
    # Reverting Team Info
    print(f"[*] Reverting Team Update...")
    db.updateTeamCity(conn, "SF", "Santa Clara") # Adjust to original value if different
    db.updateTeamName(conn, "SF", "San Francisco 49ers")         # Adjust to original value if different

    # ---------------------------------------------------------
    # 3. TEST DATA RETRIEVAL (Queries)
    # ---------------------------------------------------------
    print("\n--- 3. Testing QUERY Functions ---")

    # Helper function to print rows comfortably
    def print_rows(title, rows):
        print(f"\n> {title}")
        if not rows:
            print("  (No results found)")
            return
        # Print first 2 rows only to keep console clean
        for i, row in enumerate(rows[:2]): 
            # Convert sqlite3.Row to dict for readable printing
            print(f"  Row {i+1}: {dict(row)}") 
        if len(rows) > 2:
            print(f"  ... ({len(rows) - 2} more rows)")

    # Test all getters
    rows = db.getPlayerIdByName(conn, "Testy McTesterson II")
    print_rows("getPlayerIdByName", rows)

    rows = db.getPlayerNameById(conn, test_player_id)
    print_rows("getPlayerNameById", rows)

    rows = db.getTop5QBsByPassingYards(conn, 2024) # Using our test season
    print_rows(f"getTop5QBsByPassingYards ({test_season})", rows)

    rows = db.getTop5RBsByRushingYards(conn, 2024)
    print_rows(f"getTop5RBsByRushingYards ({test_season})", rows)

    rows = db.getTop5WRsByReceivingYards(conn, 2024)
    print_rows(f"getTop5WRsByReceivingYards ({test_season})", rows)

    rows = db.getTopPlayersAllTimeByTouchdowns(conn, top_n=3)
    print_rows("getTopPlayersAllTimeByTouchdowns", rows)

    # Note: These might not return our test player because min_games default is usually higher
    rows = db.getQBsLowestInterceptionAvgMinTD(conn, min_games=1, min_touchdowns=1)
    print_rows("getQBsLowestInterceptionAvgMinTD (Low threshold for test)", rows)

    rows = db.getPlayersLowestInterceptionsAvg(conn, min_games=1)
    print_rows("getPlayersLowestInterceptionsAvg", rows)

    rows = db.playerQBCareerStats(conn, test_player_id)
    print_rows(f"playerQBCareerStats ({test_player_id})", rows)

    rows = db.getTeamSchedule(conn, "SF", 2024) # Use a real season for schedule
    print_rows("getTeamSchedule (SF, 2024)", rows)

    record = db.get_team_record(conn, "SF", 2024)
    print(f"\n> get_team_record (SF, 2024): {record}")

    rows = db.get_conference_passing_leaders(conn, 2024, "NFC", "West")
    print_rows("get_conference_passing_leaders (NFC West)", rows)

    rows = db.get_qb_stats_vs_opponent(conn, test_player_id, "KC")
    print_rows(f"get_qb_stats_vs_opponent ({test_player_name} vs KC)", rows)

    rows = db.get_player_matchup_history(conn, test_player_id)
    print_rows(f"get_player_matchup_history ({test_player_name})", rows)

    # ---------------------------------------------------------
    # 4. TEST DATA DELETION (Cleanup)
    # ---------------------------------------------------------
    print("\n--- 4. Testing DELETE Functions ---")

    print(f"[*] Deleting Game: {test_game_id}...")
    db.deleteGame(conn, test_game_id)

    print(f"[*] Deleting Coach: {test_coach_id}...")
    db.deleteCoach(conn, test_coach_id)

    # Delete stats specific to the test
    print(f"[*] Deleting Game Stats...")
    db.deletePlayerGameStats(conn, "Testy McTesterson II", 1, test_season)

    # Delete player (and history due to function logic)
    print(f"[*] Deleting Player: {test_player_id}...")
    db.deletePlayer(conn, test_player_id)

    print("\n==========================================")
    print("TEST SUITE COMPLETE")
    print("==========================================")

    db.closeConnection(conn, database)

if __name__ == "__main__":
    run_comprehensive_test()