# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-script tool that runs an NFL season pool: it pulls schedules, scores, and player box scores from the Tank01 NFL API (RapidAPI free tier) and writes them to a Google Sheet named "NFLPPA est. 2022", which serves as the league scoreboard. League rules and commissioner instructions live in `NFLPPA est. 2022 Template.xlsx`.

## Running

```bash
pip install -r requirements.txt
python3 main.py
```

There are no tests, linters, or build steps. `crontab.txt` shows the intended refresh schedule (weekday mornings plus Sunday afternoon/evening during games).

Running `main.py` requires two credentials that are NOT in the repo:
- A Tank01 RapidAPI key — the script has `"XXXXXX"` placeholders in the four `x-rapidapi-key` headers.
- `creds.json` — a Google service account keyfile (gspread/oauth2client) in the working directory.

A full run makes ~36 API calls for season schedules/results (2 per week x 18 weeks), plus one box-score call per game in the current week, plus a player-list call. The free API tier caps at 1,000 calls/month, which is why player stats are fetched for the current week only.

## How main.py is structured

`main.py` is one linear script with no functions; sections are marked by `###` banner comments and run in this order:

1. **Connection details** — API endpoints/headers and the gspread client.
2. **Season schedules and results** — loops weeks 1–18, collects game IDs/status and scores into parallel lists, builds two DataFrames, merges them on `Game`, and overwrites the "Raw Game Level Data - API Import" worksheet.
3. **Current-week player stats** — reads the "Week Date Mapping" worksheet to find the current week from today's date, then for each of that week's games pulls the box score and collects per-category lists (receiving, rushing, passing, kicking, defense, fantasy points, team stats). Fantasy-point scoring rules are passed as query params on the box-score call. Team turnovers are intentionally swapped (home defense is credited with the away offense's turnovers, and vice versa).
4. **Merge and publish** — the category DataFrames are outer-merged on player ID, duplicate id/name/team columns are coalesced via `bfill`, player positions are joined in from the player-list endpoint, NaNs are filled with 0 before deriving `totalyds` and `totalrushrecyds` (NaNs break the arithmetic), and the result overwrites the "Player Stats - Current Week" worksheet. A refresh timestamp is written to cell A1 of the "Timestamp" worksheet.

## Things to know when editing

- The Google Sheet name ("NFLPPA est. 2022") and worksheet tab names are hardcoded in several `client.open(...).worksheet(...)` calls; the script breaks if the sheet tabs are renamed.
- The "Week Date Mapping" worksheet stores weeks as bare numbers; the script prefixes them with `"Week "` to match the API's `gameWeek` values. If today's date falls outside all mapped ranges, `current_week` is never assigned and the script crashes.
- Stats come back from the API as strings; numeric columns are converted with `pd.to_numeric` only where needed for derived fields.
- The file is indented with tabs.
