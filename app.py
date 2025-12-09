from flask import Flask, render_template, request, g, flash, redirect, url_for
import database_functions as db
import sqlite3

app = Flask(__name__)
app.secret_key = 'super_secret_key_for_flash_messages'  # Required for flash messaging

DATABASE = 'nfl_stats.sqlite'

def get_db():
    """Opens a new database connection if there is none yet for the current application context."""
    if 'db' not in g:
        g.db = db.openConnection(DATABASE)
    return g.db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    db_conn = g.pop('db', None)
    if db_conn is not None:
        db.closeConnection(db_conn, DATABASE)

# ---------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------

@app.route('/')
def index():
    """Renders the main dashboard."""
    return render_template('index.html')

@app.route('/stats/<stat_type>')
def view_stats(stat_type):
    """Handles fetching and displaying various statistics tables."""
    conn = get_db()
    data = []
    title = ""
    headers = []
    
    # Default season for demo
    season = request.args.get('season', default=2024, type=int)

    if stat_type == 'top_qbs':
        data = db.getTop5QBsByPassingYards(conn, season)
        title = f"Top 5 QBs by Passing Yards ({season})"
        headers = ["ID", "Player Name", "Passing Yards"]
        
    elif stat_type == 'top_rbs':
        data = db.getTop5RBsByRushingYards(conn, season)
        title = f"Top 5 RBs by Rushing Yards ({season})"
        headers = ["ID", "Player Name", "Rushing Yards"]

    elif stat_type == 'top_wrs':
        data = db.getTop5WRsByReceivingYards(conn, season)
        title = f"Top 5 WRs by Receiving Yards ({season})"
        headers = ["ID", "Player Name", "Receiving Yards"]

    elif stat_type == 'all_time_tds':
        data = db.getTopPlayersAllTimeByTouchdowns(conn)
        title = "Top Players All-Time by Touchdowns"
        headers = ["ID", "Player Name", "Total TDs"]

    elif stat_type == 'lowest_int':
        data = db.getQBsLowestInterceptionAvgMinTD(conn)
        title = "QBs with Lowest Interception Avg (Min 10 TDs)"
        headers = ["ID", "Player Name", "Avg Int/Game", "Total TDs"]

    elif stat_type == 'division_winners':
        data = db.getDivisionWinners(conn, season)
        title = f"Division Winners ({season})"
        headers = ["Team", "Team Name", "Conference", "Division", "Coach", "Wins", "Total Yards", "Total TDs"]
    
    elif stat_type == 'best_coach':
        data = db.best_coach(conn)
        title = f"Best Coaching Record in the past 7 Seasons"
        headers = ["Coach Name", "Team", "Wins", "Losses", "Super Bowl Wins"]

    return render_template('index.html', stats_data=data, stats_title=title, stats_headers=headers, active_tab='stats', season=season, stat_type=stat_type)

@app.route('/team_lookup', methods=['POST'])
def team_lookup():
    """Handles fetching team schedule and record."""
    conn = get_db()
    team = request.form.get('team_ticker').upper()
    season = request.form.get('season')
    
    if not season:
        season = 2024
    
    schedule = db.getTeamSchedule(conn, team, season)
    record = db.get_team_record(conn, team, season)
    
    return render_template('index.html', 
                           schedule=schedule, 
                           record=record, 
                           team_searched=team, 
                           season_searched=season,
                           active_tab='team')

@app.route('/player_lookup', methods=['POST'])
def player_lookup():
    """Handles searching for a player and showing their details."""
    conn = get_db()
    search_term = request.form.get('player_name')
    
    # 1. Try to find the ID first
    players = db.getPlayerIdByName(conn, search_term)
    
    if not players:
        flash(f"No player found with name '{search_term}'", "danger")
        return redirect(url_for('index'))
    
    # Assuming we take the first match
    player_id = players[0]['player_id']
    real_name = players[0]['player_name']
    
    # 2. Get Career Stats
    career_stats = db.playerQBCareerStats(conn, player_id)
    
    # 3. Get Matchup History
    history = db.get_player_matchup_history(conn, player_id)
    
    return render_template('index.html', 
                           player_search_result=players[0],
                           career_stats=career_stats[0] if career_stats else None,
                           matchup_history=history,
                           active_tab='player')

# ---------------------------------------------------------------------
# MANAGEMENT ACTIONS (Add/Update/Delete)
# ---------------------------------------------------------------------

@app.route('/add_player', methods=['POST'])
def add_player_route():
    conn = get_db()
    try:
        # Extract form data
        pid = request.form['player_id']
        name = request.form['player_name']
        team = request.form['team']
        pos = request.form['position']
        # Convert numeric fields
        byear = int(request.form.get('birth_year', 2000))
        dyear = int(request.form.get('draft_year', 2022))
        dovr = int(request.form.get('draft_ovr', 1))
        h = int(request.form.get('height', 72))
        w = int(request.form.get('weight', 200))
        season = int(request.form.get('season', 2024))
        
        success = db.addPlayer(conn, pid, name, team, byear, dyear, dovr, h, w, pos, season)
        
        if success:
            flash(f"Successfully added player: {name}", "success")
        else:
            flash("Failed to add player. ID might already exist.", "danger")
            
    except Exception as e:
        flash(f"Error: {e}", "danger")
        
    return redirect(url_for('index'))

@app.route('/delete_player', methods=['POST'])
def delete_player_route():
    conn = get_db()
    pid = request.form['player_id']
    
    success = db.deletePlayer(conn, pid)
    if success:
        flash(f"Successfully deleted player ID: {pid}", "success")
    else:
        flash(f"Failed to delete player {pid}", "danger")
        
    return redirect(url_for('index'))

@app.route('/update_player', methods=['POST'])
def update_player_route():
    conn = get_db()
    pid = request.form['player_id']
    action = request.form['update_action'] # 'team', 'position', 'weight'
    
    success = False
    
    if action == 'team':
        new_team = request.form['new_value']
        success = db.updatePlayerTeam(conn, pid, new_team, 2025) # Defaulting to 2025 for move
    elif action == 'position':
        new_pos = request.form['new_value']
        success = db.updatePlayerPosition(conn, pid, new_pos)
    elif action == 'weight':
        new_w = request.form['new_value']
        success = db.updatePlayerWeight(conn, pid, new_w)
        
    if success:
        flash(f"Update successful for {pid}", "success")
    else:
        flash("Update failed.", "danger")
        
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)