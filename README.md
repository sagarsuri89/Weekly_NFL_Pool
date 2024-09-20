# What is this? 
This repository contains code for an NFL playoff pool which pulls from the free tier of [this rapidapi.com API](https://rapidapi.com/tank01/api/tank01-nfl-live-in-game-real-time-statistics-nfl). 

# What is the pool format? 
This format supports for teams/owners. Each owner picks three teams in a draft style format below.

1, 20, 26

2, 16, 29

3, 13, 30

4, 18, 25

5, 15, 27

6, 19, 22

7, 11, 28

8, 17, 21

9, 14, 23

10, 12, 24

End of season payout goes to owner with most wins in aggregate across their three teams. This format supports a second place payout as well. 

Additionally, a weekly payout is made based on the following stats (based on your three teams):

Week 1	QB w/ Most Passing Yards

Week 2	Team w/ Most Points Scored

Week 3	Def with Least Yards Against

Week 4	TE w/ Most Rec Yards

Week 5	Receiver Most Fantasy Points

Week 6	DST w/ Most Points (XP, FG, TD)

Week 7	RB w/ Most Rush/Rec Yards

Week 8	Kicker Most Points (XP, FG) 

Week 9	QB with Most Completions

Week 10	Def w/ Most Sacks

Week 11	TE Most Fantasy Points

Week 12	Def w/ Most Turnovers

Week 13	QB w/ Most Rush Yards

Week 14	Def w/ Least Points Against

Week 15	QB Most Fantasy Points

Week 16	Team w/ Least Points Scored

Week 17	Receiver w/ Most Rec Yards

Week 18	RB Most Fantasy Points

More rules can be found in the "Rules" tab of this [xlsx file](https://github.com/sagarsuri89/Weekly_NFL_Pool/blob/main/NFLPPA%20est.%202022.xlsx)

# How can I adopt this for my league? 
Annual commissioner instructions can be found in the "Commissioner Instructions" tab of this [xlsx file](https://github.com/sagarsuri89/Weekly_NFL_Pool/blob/main/NFLPPA%20est.%202022.xlsx).

For first time commissioners, you'll need to do the following: 

1) Setup an account with [this rapidapi.com API](https://rapidapi.com/tank01/api/tank01-nfl-live-in-game-real-time-statistics-nfl) and get Free Tier. This is a free plan offering a hard cap of 1,000 hits per month. This plan does NOT require a credit card but likely won't last the full season dependencing on your refresh frequency so you may need to create a second account mid season.

2) Install any dependencies listed [here](https://github.com/sagarsuri89/Weekly_NFL_Pool/blob/main/requirements.txt)

3) Setup credentials for programmatic access to google sheets ([Instructions](https://developers.google.com/sheets/api/quickstart/python)). You'll need to update the reference to your creds in the python script main.py. Also will need to update any references to gsheet tabs/files in the script. 

4) If you want to automatically run refreshes, you can setup a cronjob using this [format](https://github.com/sagarsuri89/Weekly_NFL_Pool/blob/main/crontab.txt)

Enjoy! 

![Designer (1)](https://github.com/user-attachments/assets/d3f0f7d7-036b-4015-bb04-1fcb162b30d7)
