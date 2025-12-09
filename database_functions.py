import sqlite3
from sqlite3 import Error

def openConnection(_dbFile):
    """
    Opens a connection and sets the row_factory to sqlite3.Row.
    This allows accessing columns by name: row['column_name'].
    """
    print(f"--- Connecting to: {_dbFile} ---")
    conn = None
    try:
        conn = sqlite3.connect(_dbFile)
        # This is crucial for Flask/Web Apps:
        conn.row_factory = sqlite3.Row 
        print("Connection successful")
    except Error as e:
        print(f"Connection Error: {e}")

    return conn

def closeConnection(_conn, _dbFile):
    print(f"--- Closing connection: {_dbFile} ---")
    try:
        _conn.close()
        print("Connection closed")
    except Error as e:
        print(f"Close Error: {e}")

# ==========================================
# PLAYER MANAGEMENT
# ==========================================

def addPlayer(_conn, player_id, player_name, team, birth_year, draft_year, draft_ovr, height, weight, position, season_year, week=1):
    # Split into two separate executions to use parameterized queries safely
    sql_player = """
    INSERT INTO players (player_id, player_name, birth_year, draft_year, draft_ovr, height, weight, position, season, team)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    sql_history = """
    INSERT INTO player_history (player_id, season, week, team)
    VALUES (?, ?, ?, ?);
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql_player, (player_id, player_name, birth_year, draft_year, draft_ovr, height, weight, position, season_year, team))
        cur.execute(sql_history, (player_id, season_year, week, team))
        _conn.commit()
        print(f"Success: Added player {player_name}")
        return True
    except Error as e:
        print(f"Error in addPlayer: {e}")
        return False

def updatePlayerTeam(_conn, player_id, new_team, season_year, week=1):
    sql_update = "UPDATE players SET team = ? WHERE player_id = ?;"
    sql_insert = "INSERT INTO player_history (player_id, season, week, team) VALUES (?, ?, ?, ?);"
    try:
        cur = _conn.cursor()
        cur.execute(sql_update, (new_team, player_id))
        cur.execute(sql_insert, (player_id, season_year, week, new_team))
        _conn.commit()
        print(f"Success: Moved player {player_id} to {new_team}")
        return True
    except Error as e:
        print(f"Error in updatePlayerTeam: {e}")
        return False

def updatePlayerPosition(_conn, player_id, new_position):
    sql = "UPDATE players SET position = ? WHERE player_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (new_position, player_id))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in updatePlayerPosition: {e}")
        return False

def updatePlayerWeight(_conn, player_id, new_weight):
    sql = "UPDATE players SET weight = ? WHERE player_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (new_weight, player_id))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in updatePlayerWeight: {e}")
        return False

def updatePlayerName(_conn, player_id, new_name):
    sql = "UPDATE players SET player_name = ? WHERE player_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (new_name, player_id))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in updatePlayerName: {e}")
        return False

def deletePlayer(_conn, player_id): 
    # Using parameterized queries for deletion
    sql1 = "DELETE FROM player_history WHERE player_id = ?;"
    sql2 = "DELETE FROM players WHERE player_id = ?;"
    sql3 = "DELETE FROM player_game_stats WHERE player_id = ?;"
    
    try:
        cur = _conn.cursor()
        cur.execute(sql1, (player_id,))
        cur.execute(sql2, (player_id,))
        cur.execute(sql3, (player_id,))
        _conn.commit()
        print(f"Success: Deleted player {player_id}")
        return True
    except Error as e:
        print(f"Error in deletePlayer: {e}")
        return False

def deletePlayerGameStats(_conn, player_name, week, season):
    sql = "DELETE FROM player_game_stats WHERE player_name = ? AND week = ? AND season = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_name, week, season))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in deletePlayerGameStats: {e}")
        return False

def addPlayerGameStats(_conn, season, player_id, player_name, week, team,
                       receptions=0.0, interception=0.0, rush_touchdown=0.0,
                       pass_touchdown=0.0, receiving_touchdown=0.0,
                       passing_yards=0.0, rushing_yards=0.0, receiving_yards=0.0,
                       fumble=0.0, fumble_lost=0.0, safety=0.0):
    sql = """
    INSERT INTO player_game_stats (
        season, player_id, player_name, week, team,
        receptions, interception, rush_touchdown, pass_touchdown, receiving_touchdown,
        passing_yards, rushing_yards, receiving_yards, fumble, fumble_lost, safety
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        season, player_id, player_name, week, team,
        receptions, interception, rush_touchdown, pass_touchdown, receiving_touchdown,
        passing_yards, rushing_yards, receiving_yards, fumble, fumble_lost, safety
    )
    try:
        cur = _conn.cursor()
        cur.execute(sql, params)
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in addPlayerGameStats: {e}")
        return False

# ==========================================
# COACH MANAGEMENT
# ==========================================

def addCoach(_conn, coach_name, coach_id, team, hire_year):
    sql1 = "UPDATE coaches SET coach_id = ?, name = ? WHERE team = ?;"
    sql2 = "INSERT INTO coach_history (season, coach_id, name, team) VALUES (?, ?, ?, ?);"
    try:
        cur = _conn.cursor()
        cur.execute(sql1, (coach_id, coach_name, team))
        cur.execute(sql2, (hire_year, coach_id, coach_name, team))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in addCoach: {e}")
        return False

def deleteCoach(_conn, coach_id):
    sql1 = "DELETE FROM coach_history WHERE coach_id = ?;"
    sql2 = "UPDATE coaches SET coach_id = (SELECT MIN(coach_id)-1 FROM coaches), name = 'Vacant' WHERE coach_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql1, (coach_id,))
        cur.execute(sql2, (coach_id,))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in deleteCoach: {e}")
        return False

# ==========================================
# TEAM MANAGEMENT
# ==========================================

def updateTeamCity(_conn, team_id, new_city):
    sql = "UPDATE teams SET city = ? WHERE team = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (new_city, team_id))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in updateTeamCity: {e}")
        return False

def updateTeamName(_conn, team_id, new_name):
    sql = "UPDATE teams SET team_name = ? WHERE team = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (new_name, team_id))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in updateTeamName: {e}")
        return False

# ==========================================
# GAME MANAGEMENT
# ==========================================

def addGame(_conn, game_id, season, week, season_type, away_team, home_team, home_win):
    sql = """
    INSERT INTO games (game_id, season, week, season_type, away_team, home_team, home_win)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (game_id, season, week, season_type, away_team, home_team, home_win))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in addGame: {e}")
        return False

def deleteGame(_conn, game_id):
    sql = "DELETE FROM games WHERE game_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (game_id,))
        _conn.commit()
        return True
    except Error as e:
        print(f"Error in deleteGame: {e}")
        return False

# ==========================================
# QUERIES (Updated to return data)
# ==========================================

def getPlayerIdByName(_conn, player_name):
    sql = """
    SELECT player_id, player_name, position
    FROM players
    WHERE player_name = ?
    ORDER BY position DESC
    LIMIT 1;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_name,))
        rows = cur.fetchall()
        return rows # Returns a list of sqlite3.Row objects
    except Error as e:
        print(f"Error in getPlayerIdByName: {e}")
        return []

def getTop5QBsByPassingYards(_conn, season_year):
    sql = """
    SELECT player_id, player_name, SUM(passing_yards) AS total_passing_yards
    FROM player_game_stats
    WHERE season = ?
    GROUP BY player_id, player_name
    ORDER BY total_passing_yards DESC
    LIMIT 5;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (season_year,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getTop5QBsByPassingYards: {e}")
        return []

def getTop5RBsByRushingYards(_conn, season_year):
    sql = """
    SELECT p.player_id, p.player_name, SUM(s.rushing_yards) AS total_rushing_yards
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    WHERE s.season = ? AND p.position = 'RB'
    GROUP BY p.player_id, p.player_name
    ORDER BY total_rushing_yards DESC
    LIMIT 5;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (season_year,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getTop5RBsByRushingYards: {e}")
        return []

def getTop5WRsByReceivingYards(_conn, season_year):
    sql = """
    SELECT p.player_id, p.player_name, SUM(s.receiving_yards) AS total_receiving_yards
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    WHERE s.season = ? AND p.position = 'WR'
    GROUP BY p.player_id, p.player_name
    ORDER BY total_receiving_yards DESC
    LIMIT 5;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (season_year,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getTop5WRsByReceivingYards: {e}")
        return []

def getTopPlayersAllTimeByTouchdowns(_conn, top_n=5):
    sql = """
    SELECT p.player_id, p.player_name,
           SUM(s.rush_touchdown + s.pass_touchdown + s.receiving_touchdown) AS total_touchdowns
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    GROUP BY p.player_id, p.player_name
    ORDER BY total_touchdowns DESC
    LIMIT ?
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (top_n,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getTopPlayersAllTimeByTouchdowns: {e}")
        return []

def getQBsLowestInterceptionAvgMinTD(_conn, min_games=12, min_touchdowns=10, top_n=10):
    sql = """
    SELECT p.player_id, p.player_name,
           SUM(s.interception) * 1.0 / COUNT(s.week) AS avg_interceptions,
           SUM(s.pass_touchdown + s.rush_touchdown + s.receiving_touchdown) AS total_touchdowns
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    WHERE p.position = 'QB'
    GROUP BY p.player_id, p.player_name
    HAVING COUNT(s.week) >= ? AND total_touchdowns >= ?
    ORDER BY avg_interceptions ASC
    LIMIT ?
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (min_games, min_touchdowns, top_n))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getQBsLowestInterceptionAvgMinTD: {e}")
        return []

def getPlayersLowestInterceptionsAvg(_conn, min_games=1, top_n=5):
    sql = """
    SELECT p.player_id, p.player_name,
           SUM(s.interception) * 1.0 / COUNT(s.player_id) AS avg_interceptions
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    GROUP BY p.player_id, p.player_name
    HAVING COUNT(s.player_id) >= ?
    ORDER BY avg_interceptions ASC
    LIMIT ?
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (min_games, top_n))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getPlayersLowestInterceptionsAvg: {e}")
        return []

def getPlayerNameById(_conn, player_id):
    sql = "SELECT player_name FROM players WHERE player_id = ?;"
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_id,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getPlayerNameById: {e}")
        return []

def playerQBCareerStats(_conn, player_id):
    sql = """
    SELECT 
        SUM(passing_yards) AS total_passing_yards,
        SUM(rushing_yards) AS total_rushing_yards,
        SUM(pass_touchdown) AS total_pass_touchdowns,
        SUM(rush_touchdown) AS total_rush_touchdowns,
        sum(receiving_yards) AS total_receiving_yards,
        SUM(receiving_touchdown) AS total_receiving_touchdowns,
        SUM(interception) AS total_interceptions
    FROM player_game_stats
    WHERE player_id = ?;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_id,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in playerQBCareerStats: {e}")
        return []

def getTeamSchedule(_conn, team, season):
    # Renamed from 'printTeamSchedule' to 'getTeamSchedule'
    sql = """
    SELECT week, season_type, away_team, home_team
    FROM games
    WHERE (away_team = ? OR home_team = ?) AND (season = ? AND season_type = 'REG')
    ORDER BY week ASC;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (team, team, season))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in getTeamSchedule: {e}")
        return []

def get_team_record(conn, team, season):
    """ Returns a dictionary with wins and losses """
    sql = """
        SELECT
            SUM(
                CASE
                    WHEN home_team = ? AND home_win = 1 THEN 1     -- Home win
                    WHEN away_team = ? AND home_win = 0 THEN 1     -- Away win
                    ELSE 0
                END
            ) AS wins,
            SUM(
                CASE
                    WHEN home_team = ? AND home_win = 0 THEN 1     -- Home loss
                    WHEN away_team = ? AND home_win = 1 THEN 1     -- Away loss
                    ELSE 0
                END
            ) AS losses
        FROM games
        WHERE season = ?;
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (team, team, team, team, season))
        row = cur.fetchone()
        return {'wins': row['wins'], 'losses': row['losses']}
    except Error as e:
        print(f"Error in get_team_record: {e}")
        return {'wins': 0, 'losses': 0}

def get_conference_passing_leaders(_conn, season, conference, division, top_n=5):
    sql = """
    SELECT p.player_name, t.team_name, SUM(s.passing_yards) as total_yards
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    JOIN teams t ON s.team = t.team
    WHERE s.season = ? 
      AND t.conference = ? 
      AND t.division = ?
      AND p.position = 'QB'
    GROUP BY p.player_id, p.player_name
    ORDER BY total_yards DESC
    LIMIT ?;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (season, conference, division, top_n))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in get_conference_passing_leaders: {e}")
        return []

def get_qb_stats_vs_opponent(_conn, player_id, opponent_team_ticker):
    sql = """
    SELECT 
        p.player_name,
        COUNT(g.game_id) as games_played,
        AVG(s.passing_yards) as avg_pass_yards,
        AVG(s.pass_touchdown) as avg_pass_tds,
        AVG(s.interception) as avg_ints
    FROM player_game_stats s
    JOIN players p ON s.player_id = p.player_id
    JOIN games g ON s.season = g.season AND s.week = g.week
    WHERE s.player_id = ?
      AND (
          (g.home_team = s.team AND g.away_team = ?) 
          OR 
          (g.away_team = s.team AND g.home_team = ?)
      );
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_id, opponent_team_ticker, opponent_team_ticker))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in get_qb_stats_vs_opponent: {e}")
        return []

def get_player_matchup_history(_conn, player_id):
    """
    Returns a detailed game log for a player.
    """
    sql = """
    SELECT 
        s.season,
        s.week,
        opp_t.team_name AS opponent,
        c.name AS opposing_coach,
        CASE 
            WHEN (s.team = g.home_team AND g.home_win = 1) OR (s.team = g.away_team AND g.home_win = 0) 
            THEN 'Win'
            ELSE 'Loss'
        END AS game_result,
        s.passing_yards,
        s.rushing_yards,
        s.receiving_yards
    FROM player_game_stats s
    -- 1. Join Games to get schedule info (Home/Away logic)
    JOIN games g 
        ON s.season = g.season 
        AND s.week = g.week 
        AND (s.team = g.home_team OR s.team = g.away_team)
    -- 2. Join Teams to get the Opponent's details
    JOIN teams opp_t 
        ON opp_t.team = (CASE WHEN s.team = g.home_team THEN g.away_team ELSE g.home_team END)
    -- 3. Join Coach History to find who coached the opponent that year
    LEFT JOIN coach_history ch 
        ON ch.team = opp_t.team 
        AND ch.season = s.season
    -- 4. Join Coaches to get the coach's name
    LEFT JOIN coaches c 
        ON ch.coach_id = c.coach_id
    WHERE s.player_id = ?
    ORDER BY s.season DESC, s.week DESC;
    """
    try:
        cur = _conn.cursor()
        cur.execute(sql, (player_id,))
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(f"Error in get_player_matchup_history: {e}")
        return []
    
def getDivisionWinners(conn, season):
    sql = """
    WITH team_wins AS (
        SELECT
            t.team,
            t.division,
            t.conference,
            SUM(
                CASE 
                    WHEN g.home_team = t.team AND g.home_win = 1 THEN 1
                    WHEN g.away_team = t.team AND g.home_win = 0 THEN 1
                    ELSE 0
                END
            ) AS wins
        FROM teams t
        JOIN games g ON t.team IN (g.home_team, g.away_team)
        WHERE g.season = ?
        GROUP BY t.team, t.division, t.conference
    ),

    team_stats AS (
        SELECT 
            t.team,
            SUM(s.passing_yards + s.rushing_yards + s.receiving_yards) AS total_yards,
            SUM(s.pass_touchdown + s.rush_touchdown + s.receiving_touchdown) AS total_tds
        FROM teams t
        JOIN player_game_stats s ON t.team = s.team
        WHERE s.season = ?
        GROUP BY t.team
    ),

    team_info AS (
        SELECT
            t.team,
            t.team_name,
            t.division,
            t.conference,
            c.name AS coach_name,
            COALESCE(w.wins, 0) AS wins,
            COALESCE(s.total_yards, 0) AS total_yards,
            COALESCE(s.total_tds, 0) AS total_tds
        FROM teams t
        LEFT JOIN team_wins w ON t.team = w.team
        LEFT JOIN team_stats s ON t.team = s.team
        LEFT JOIN coaches c ON t.team = c.team
    )

    SELECT *
    FROM team_info i
    WHERE (i.team, i.division) IN (
        SELECT team, division
        FROM (
            SELECT team, division, 
                   RANK() OVER (PARTITION BY conference, division ORDER BY wins DESC, total_yards DESC) AS div_rank
            FROM team_info
        ) ranked
        WHERE div_rank = 1
    )
    ORDER BY conference, division;
    """

    cur = conn.cursor()
    cur.execute(sql, (season, season))
    rows = cur.fetchall()

    
    return rows

def best_coach(conn):
    sql = """
    SELECT
        c.name AS coach_name,
        ch.team AS team,
        SUM(CASE WHEN g.team = ch.team AND g.win = 1 THEN 1 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN g.team = ch.team AND g.win = 0 THEN 1 ELSE 0 END) AS total_losses,
        SUM(CASE WHEN g.week = 22 AND g.team = ch.team AND g.win = 1 THEN 1 ELSE 0 END) AS super_bowl_wins
    FROM coach_history ch
    JOIN coaches c ON ch.coach_id = c.coach_id
    JOIN (
        SELECT home_team AS team, home_win AS win, week, season
        FROM games
        UNION ALL
        SELECT away_team AS team, CASE WHEN home_win = 0 THEN 1 ELSE 0 END AS win, week, season
        FROM games
    ) g ON g.team = ch.team AND g.season = ch.season
    GROUP BY c.name, ch.team
    ORDER BY total_wins DESC
    LIMIT 5;
    """
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()

def getPlayerCareerDetails(_conn, player_id, include_passing=False, include_rushing=False, include_receiving=False, include_turnovers=False):
    """
    Returns a dictionary containing:
    - 'bio': All columns from the player table.
    - 'teams': List of teams played for and the year they first signed/played.
    - 'career_stats': Total games played and aggregated stats based on booleans provided.
    """
    result = {
        "bio": {},
        "teams": [],
        "career_stats": {"total_games_played": 0}
    }

    try:
        cur = _conn.cursor()

        # ---------------------------------------------------------
        # 1. GET PLAYER BIO
        # ---------------------------------------------------------
        # [cite_start]Retrieves all columns from the players table [cite: 4, 5, 6, 7]
        sql_bio = "SELECT * FROM players WHERE player_id = ?;"
        cur.execute(sql_bio, (player_id,))
        bio_row = cur.fetchone()

        if bio_row:
            # Convert sqlite3.Row object to a standard dictionary
            result["bio"] = dict(bio_row)
        else:
            print(f"Player {player_id} not found.")
            return None

        # ---------------------------------------------------------
        # 2. GET TEAM HISTORY (Year Signed)
        # ---------------------------------------------------------
        # [cite_start]Uses player_history to find the earliest season (min) a player appeared for each team [cite: 13, 14, 15]
        sql_teams = """
        SELECT team, MIN(season) as year_signed
        FROM player_history
        WHERE player_id = ?
        GROUP BY team
        ORDER BY year_signed ASC;
        """
        cur.execute(sql_teams, (player_id,))
        team_rows = cur.fetchall()
        
        # Format as a list of dictionaries for readability
        for row in team_rows:
            result["teams"].append({
                "team": row["team"], 
                "year_signed": row["year_signed"]
            })

        # ---------------------------------------------------------
        # 3. GET CAREER TOTALS & GAMES PLAYED
        # ---------------------------------------------------------
        # [cite_start]Calculates total games and aggregates specific stats from player_game_stats [cite: 19, 20, 21, 22]
        
        # Base selection for games played
        select_clause = "COUNT(*) as games_played"
        
        # Dynamically add aggregations based on booleans
        if include_passing:
            select_clause += ", SUM(passing_yards) as total_passing_yards, SUM(pass_touchdown) as total_pass_tds"
        
        if include_rushing:
            select_clause += ", SUM(rushing_yards) as total_rushing_yards, SUM(rush_touchdown) as total_rush_tds"
            
        if include_receiving:
            select_clause += ", SUM(receiving_yards) as total_receiving_yards, SUM(receiving_touchdown) as total_receiving_tds, SUM(receptions) as total_receptions"
            
        if include_turnovers:
            select_clause += ", SUM(interception) as total_interceptions, SUM(fumble) as total_fumbles, SUM(fumble_lost) as total_fumbles_lost"

        sql_stats = f"SELECT {select_clause} FROM player_game_stats WHERE player_id = ?;"
        
        cur.execute(sql_stats, (player_id,))
        stats_row = cur.fetchone()

        if stats_row:
            # Populate the career_stats dictionary
            result["career_stats"]["total_games_played"] = stats_row["games_played"]
            
            # Helper to safely add stats (handling None if sum returns NULL)
            def safe_add(key):
                if key in stats_row.keys():
                    result["career_stats"][key] = stats_row[key] if stats_row[key] is not None else 0

            # Add the requested stats to the result
            if include_passing:
                safe_add("total_passing_yards")
                safe_add("total_pass_tds")
            if include_rushing:
                safe_add("total_rushing_yards")
                safe_add("total_rush_tds")
            if include_receiving:
                safe_add("total_receiving_yards")
                safe_add("total_receiving_tds")
                safe_add("total_receptions")
            if include_turnovers:
                safe_add("total_interceptions")
                safe_add("total_fumbles")
                safe_add("total_fumbles_lost")

        return result

    except Error as e:
        print(f"Error in getPlayerCareerDetails: {e}")
        return None


# ==========================================
# TEST FUNCTIONS
# ==========================================

def addDummyPlayer(_conn):
    addPlayer(_conn,"00-0039922", 'John Doe', 'SF', 1990, 2012, 15, 72, 210, 'WR', 2025)
    addPlayerGameStats(_conn, 2025, "00-0039922", 'John Doe', 1, 'SF', receptions=5, receiving_yards=80, receiving_touchdown=1)

def main():
    database = r"nfl_stats.sqlite"
    conn = openConnection(database)

    # Example: How to use the returned data in Flask or Terminal
    if conn:
        print("\n--- Top 5 QBs (Raw Data) ---")
        qbs = getTop5QBsByPassingYards(conn)
        for row in qbs:
            # You can now access data by name!
            print(f"{row['player_name']}: {row['total_passing_yards']}")

        print("\n--- Team Record (Structured Data) ---")
        record = get_team_record(conn, 'SF', 2024)
        print(f"SF 2024 Record: {record['wins']} - {record['losses']}")

        closeConnection(conn, database)

if __name__ == '__main__':
    main()