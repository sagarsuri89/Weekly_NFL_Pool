from datetime import date
import requests
import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import datetime
import numpy as np

#############################################################################################
# CONNECTION DETAILS #
#############################################################################################
# query params for schedule call
url_sched = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForWeek"
headers_sched = {
	"x-rapidapi-key": "995ddaeee9msh0dd1fd247fbb15dp1b6ddfjsn53c1991d7c79",
	"x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
}

# query params for results call
url_results = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLScoresOnly"
headers_results = {
	"x-rapidapi-key": "995ddaeee9msh0dd1fd247fbb15dp1b6ddfjsn53c1991d7c79",
	"x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
}

# query params for player stats call
url_player_stats = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLBoxScore"
headers_player_stats = {
	"x-rapidapi-key": "995ddaeee9msh0dd1fd247fbb15dp1b6ddfjsn53c1991d7c79",
	"x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
}

# query params for player list call
url_player_list = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
headers_player_list = {
	"x-rapidapi-key": "995ddaeee9msh0dd1fd247fbb15dp1b6ddfjsn53c1991d7c79",
	"x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
}

# google sheets scope
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
		 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)

#############################################################################################
# SETTING GROUNDWORK FOR UPCOMING LOOPS FOR SEASON SCHEDULES AND RESULTS #
#############################################################################################
# to loop through each week
week_counter = list(range(1, 19))

# lists for columns from schedule call
game = []
week = []
gameStatusCode = []

# lists for columns from results call
gameID = []
away = []
home = []
awaypts = []
homepts = []

#############################################################################################
# START LOOP FOR SEASON SCHEDULES AND RESULTS #
#############################################################################################
for week_number in week_counter:

	#############################################################################################
	# LIST OF GAMES BY WEEK #
	#############################################################################################
	querystring_sched = {"week": week_number, "seasonType": "reg"}
	response = requests.get(url_sched, headers=headers_sched, params=querystring_sched)
	input_dict_sched = response.json()
	body_dict_sched = input_dict_sched["body"]
	# CREATE LIST FOR EACH COLUMN
	for i in body_dict_sched:
		game.append((i['gameID']))
		week.append((i['gameWeek']))
		gameStatusCode.append((i['gameStatusCode']))

	#############################################################################################
	# LIST OF RESULTS BY WEEK #
	#############################################################################################
	querystring_results = {"gameWeek": week_number, "seasonType": "reg"}
	response = requests.get(url_results, headers=headers_results, params=querystring_results)
	input_dict_results = response.json()
	body_dict_results = input_dict_results["body"]
	for key, value in body_dict_results.items():
		gameID.append(key)
		game_dict = value
		away.append((game_dict['away']))
		home.append((game_dict['home']))
		awaypts.append((game_dict['awayPts']))
		homepts.append((game_dict['homePts']))

#############################################################################################
# CREATE DATAFRAME TABLES FOR SEASON SCHEDULES AND RESULTS #
#############################################################################################
# MERGE SCHEDULE LISTS INTO A DATAFRAME TABLE
gamelog_complete = pd.DataFrame(
	{'Game': game,
	 'Week': week,
	 'Status': gameStatusCode
	 })

# FILTER FOR ONLY COMPLETED GAMES
# gamelog_complete = gamelog[gamelog.Status == '2']

# MERGE RESULTS LISTS INTO A DATAFRAME TABLE
full_game_results = pd.DataFrame(
	{'Game': gameID,
	 'Away_Team': away,
	 'Home_Team': home,
	 'Away_Pts': awaypts,
	 'Home_Pts': homepts
	 })

#############################################################################################
# JOIN DATAFRAME TABLES FOR SEASON SCHEDULES AND RESULTS #
#############################################################################################

df_merged = full_game_results.merge(gamelog_complete, on='Game', how='left')

#############################################################################################
# WRITE SEASON SCHEDULES AND RESULTS TO GSHEET #
# https://docs.google.com/spreadsheets/d/1nAB4oWA_Z2OlfafXAET78w79PCBTfcdCGC-Nl-GK2Ug/edit?gid=2141469202#gid=2141469202 #
#############################################################################################

sheet = client.open("NFLPPA est. 2022").worksheet("Raw Game Level Data - API Import")
sheet.clear()
set_with_dataframe(worksheet=sheet, dataframe=df_merged, include_index=False
				   #, include_column_header=True, resize=True
				   )

#############################################################################################
# GET PLAYER STATS FOR CURRENT WEEK ONLY TO LIMIT API CALLS #
#############################################################################################

# pull weeks and associated dates from gsheet
sheet = client.open("NFLPPA est. 2022").worksheet("Week Date Mapping")
data = sheet.get_all_values()
headers = data.pop(0)
df = pd.DataFrame(data, columns=headers)

# convert dataframe dates to proper format
df["Start_Date"] = pd.to_datetime(df["Start_Date"]).dt.date
df["End_Date"] = pd.to_datetime(df["End_Date"]).dt.date

# find current week
for index, row in df.iterrows():
	if date.today() >= row['Start_Date'] and date.today() <= row['End_Date']:
		current_week = "Week " + row['Week']
		break

# filter for current week gameID's
current_week_games = gamelog_complete[gamelog_complete.Week == current_week]

# create lists to capture player stats - more efficient than appending to dataframe
receiving_list = []
rushing_list = []
passing_list = []
kicking_list = []
def_list = []
fantasy_list = []
team_stats_list = []

# loop through each game in current week and pull player stats
for index, row in current_week_games.iterrows():
	current_week_game_id = row['Game']
	querystring_player_stats = {"gameID": current_week_game_id, "fantasyPoints": "true", "twoPointConversions": "2",
								"passYards": ".04", "passAttempts": "0", "passTD": "4", "passCompletions": "0",
								"passInterceptions": "-2", "pointsPerReception": "0", "carries": "0", "rushYards": ".1",
								"rushTD": "6", "fumbles": "-2", "receivingYards": ".1", "receivingTD": "6",
								"targets": "0",
								"defTD": "6", "fgMade": "3", "fgMissed": "0", "xpMade": "1", "xpMissed": "0"}
	response = requests.get(url_player_stats, headers=headers_player_stats, params=querystring_player_stats)
	input_dict_player_stats = response.json()
	body_dict_player_stats = input_dict_player_stats["body"]
	if "playerStats" in body_dict_player_stats:
		player_stats_dict = body_dict_player_stats["playerStats"]

		# loop players that played in the game and detect/collect relevant stats
		for i in player_stats_dict:

			# collect receiving stats
			if "Receiving" in player_stats_dict[i]:
				receiver_stats_dict = player_stats_dict[i]
				receiving_stats_dict = receiver_stats_dict["Receiving"]
				rectd = receiving_stats_dict["recTD"]
				recyds = receiving_stats_dict["recYds"]
				rename = receiver_stats_dict["longName"]
				reteam = receiver_stats_dict["team"]
				replayer_id = receiver_stats_dict["playerID"]
				receiving_list.append([replayer_id, rename, reteam, recyds, rectd])

			# collect rushing stats
			if "Rushing" in player_stats_dict[i]:
				rusher_stats_dict = player_stats_dict[i]
				rushing_stats_dict = rusher_stats_dict["Rushing"]
				rushtd = rushing_stats_dict["rushTD"]
				rushyds = rushing_stats_dict["rushYds"]
				rname = rusher_stats_dict["longName"]
				rteam = rusher_stats_dict["team"]
				rplayer_id = rusher_stats_dict["playerID"]
				rushing_list.append([rplayer_id, rname, rteam, rushyds, rushtd])

			# collect passing stats
			if "Passing" in player_stats_dict[i]:
				passer_stats_dict = player_stats_dict[i]
				passing_stats_dict = passer_stats_dict["Passing"]
				passtd = passing_stats_dict["passTD"]
				passyds = passing_stats_dict["passYds"]
				int = passing_stats_dict["int"]
				passcmp = passing_stats_dict["passCompletions"]
				pname = passer_stats_dict["longName"]
				pteam = passer_stats_dict["team"]
				pplayer_id = passer_stats_dict["playerID"]
				passing_list.append([pplayer_id, pname, pteam, passyds, passtd, int, passcmp])

			# collect kicking stats
			if "Kicking" in player_stats_dict[i]:
				kicker_stats_dict = player_stats_dict[i]
				kicking_stats_dict = kicker_stats_dict["Kicking"]
				if "kickingPts" in kicking_stats_dict:
					kicking_pts = kicking_stats_dict["kickingPts"]
				kname = kicker_stats_dict["longName"]
				kteam = kicker_stats_dict["team"]
				kplayer_id = kicker_stats_dict["playerID"]
				kicking_list.append([kplayer_id, kname, kteam, kicking_pts])

			# collect defensive stats
			if "Defense" in player_stats_dict[i]:
				defender_stats_dict = player_stats_dict[i]
				def_stats_dict = defender_stats_dict["Defense"]
				if "fumblesLost" in def_stats_dict:
					fumbles = def_stats_dict["fumblesLost"]
				if "fumblesLost" not in def_stats_dict:
					fumbles = np.nan
				if "sacks" in def_stats_dict:
					sacks = def_stats_dict["sacks"]
				if "sacks" not in def_stats_dict:
					sacks = np.nan
				if "defensiveInterceptions" in def_stats_dict:
					defensiveints = def_stats_dict["defensiveInterceptions"]
				if "defensiveInterceptions" not in def_stats_dict:
					defensiveints = np.nan
				dname = defender_stats_dict["longName"]
				dteam = defender_stats_dict["team"]
				dplayer_id = defender_stats_dict["playerID"]
				def_list.append([dplayer_id, dname, dteam, fumbles, sacks, defensiveints])

			# collect pre-calculated fantasy points based on rules provided in api query params
			fantasy_player_dict = player_stats_dict[i]
			if "fantasyPoints" in fantasy_player_dict:
				fanplayer_id = fantasy_player_dict["playerID"]
				fantasy_points = fantasy_player_dict["fantasyPoints"]
				fanname = fantasy_player_dict["longName"]
				fanteam = fantasy_player_dict["team"]
			if "fantasyPoints" not in fantasy_player_dict:
				fanplayer_id = fantasy_player_dict["playerID"]
				fantasy_points = np.nan
				fanname = fantasy_player_dict["longName"]
				fanteam = fantasy_player_dict["team"]
			fantasy_list.append([fanplayer_id, fanname, fanteam, fantasy_points])

	if "teamStats" in body_dict_player_stats:
		# pull turnovers by team
		team_stats_dict = body_dict_player_stats["teamStats"]

		home_team_stats_dict = team_stats_dict["home"]
		away_team_stats_dict = team_stats_dict["away"]

		home_player_id = "TEAM STAT"
		home_team = home_team_stats_dict["team"]
		home_dst_td = home_team_stats_dict["defensiveOrSpecialTeamsTds"]
		# switch to away so defense is credited with turnovers
		home_turnovers = away_team_stats_dict["turnovers"]

		away_player_id = "TEAM STAT"
		away_team = away_team_stats_dict["team"]
		away_dst_td = away_team_stats_dict["defensiveOrSpecialTeamsTds"]
			# switch to home so defense is credited with turnovers
		away_turnovers = home_team_stats_dict["turnovers"]

		team_stats_list.append([home_player_id, home_team, home_dst_td, home_turnovers])
		team_stats_list.append([away_player_id, away_team, away_dst_td, away_turnovers])

# convert lists to data frames
receiving_df = pd.DataFrame(receiving_list, columns=['replayer_id', 'rename', 'reteam', 'recyds', 'rectd'])
rushing_df = pd.DataFrame(rushing_list, columns=['rplayer_id', 'rname', 'rteam', 'rushyds', 'rushtd'])
passing_df = pd.DataFrame(passing_list, columns=['pplayer_id', 'pname', 'pteam', 'passyds', 'passtd', 'int', 'passcmp'])
kicking_df = pd.DataFrame(kicking_list, columns=['kplayer_id', 'kname', 'kteam', 'kicking_pts'])
def_df = pd.DataFrame(def_list, columns=['fplayer_id', 'fname', 'fteam', 'fumbles', 'sacks', 'defensiveints'])
fantasy_df = pd.DataFrame(fantasy_list, columns=['fanplayer_id', 'fanname', 'fanteam', 'fantasy_points'])
team_stats_df = pd.DataFrame(team_stats_list, columns=['team_player_id', 'teamname', 'dst_td', 'turnovers'])

# merge data frames
df_merged = receiving_df.merge(rushing_df, left_on=['replayer_id'], right_on=['rplayer_id'], how='outer')
df_merged = df_merged.merge(passing_df, left_on=['rplayer_id'], right_on=['pplayer_id'], how='outer')
df_merged = df_merged.merge(kicking_df, left_on=['pplayer_id'], right_on=['kplayer_id'], how='outer')
df_merged = df_merged.merge(def_df, left_on=['kplayer_id'], right_on=['fplayer_id'], how='outer')
df_merged = df_merged.merge(fantasy_df, left_on=['fplayer_id'], right_on=['fanplayer_id'], how='outer')
df_merged = df_merged.merge(team_stats_df, left_on=['fanplayer_id'], right_on=['team_player_id'], how='outer')

# drop dupe columns
df_merged['player_id'] = df_merged[
							 ['replayer_id', 'rplayer_id', 'pplayer_id', 'kplayer_id', 'fplayer_id', 'fanplayer_id',
							  'team_player_id']].bfill(axis=1).iloc[:, 0]
df_merged = df_merged.drop(
	columns=['replayer_id', 'rplayer_id', 'pplayer_id', 'kplayer_id', 'fplayer_id', 'fanplayer_id', 'team_player_id'])
df_merged['name'] = df_merged[['rename', 'rname', 'pname', 'kname', 'fname', 'fanname']].bfill(axis=1).iloc[:, 0]
df_merged = df_merged.drop(columns=['rename', 'rname', 'pname', 'kname', 'fname', 'fanname'])
df_merged['team'] = df_merged[['reteam', 'rteam', 'pteam', 'kteam', 'fteam', 'fanteam', 'teamname']].bfill(axis=1).iloc[
					:, 0]
df_merged = df_merged.drop(columns=['reteam', 'rteam', 'pteam', 'kteam', 'fteam', 'fanteam', 'teamname'])

# bring in position
player_id_from_list = []
player_position = []
response = requests.get(url_player_list, headers=headers_player_list)
input_dict_player_list = response.json()
body_dict_player_list = input_dict_player_list["body"]
for i in body_dict_player_list:
	player_id_from_list.append((i['playerID']))
	player_position.append((i['pos']))
player_list = pd.DataFrame(
	{'plplayer_id': player_id_from_list,
	 'position': player_position,
	 })
df_merged = df_merged.merge(player_list, left_on=['player_id'], right_on=['plplayer_id'], how='left')
df_merged = df_merged.drop(columns=['plplayer_id'])

# reorder columns, sort records, drop null records
df_merged = df_merged[
	['player_id', 'name', 'team', 'position', 'recyds', 'rectd', 'rushyds', 'rushtd', 'passyds', 'passtd', 'int',
	 'kicking_pts', 'fumbles', 'passcmp', 'sacks', 'defensiveints', 'fantasy_points', 'dst_td', 'turnovers']]
df_merged = df_merged.sort_values(by=['passyds'], ascending=False)
df_merged = df_merged.sort_values(by=['rushyds'], ascending=False)
df_merged = df_merged.sort_values(by=['recyds'], ascending=False)
df_merged = df_merged.dropna(axis=0, how='all',
							 subset=['recyds', 'rectd', 'rushyds', 'rushtd', 'passyds', 'passtd', 'int', 'kicking_pts',
									 'fumbles', 'passcmp', 'sacks', 'defensiveints', 'fantasy_points', 'dst_td',
									 'turnovers'])

# append derived columns
df_merged['rushyds'] = pd.to_numeric(df_merged['rushyds']).astype('Int64')
df_merged['passyds'] = pd.to_numeric(df_merged['passyds']).astype('Int64')
df_merged['recyds'] = pd.to_numeric(df_merged['recyds']).astype('Int64')


df_merged['totalyds'] = df_merged['rushyds'] + df_merged['passyds']
df_merged['totalrushrecyds'] = df_merged['rushyds'] + df_merged['recyds']

# post to gsheet
sheet = client.open("NFLPPA est. 2022").worksheet("Player Stats - Current Week")
sheet.clear()
set_with_dataframe(worksheet=sheet, dataframe=df_merged, include_index=False
				   #, include_column_header=True, resize=True
				   )


#post refresh time to summary tab
sheet2 = client.open("NFLPPA est. 2022").worksheet("Timestamp")
date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
sheet2.update_acell('A1', date_now)
