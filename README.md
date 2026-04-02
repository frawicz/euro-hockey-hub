# Euro Hockey Hub

Multi-league European ice hockey data platform. Scrapes game data, player stats, and events from 6 leagues and serves them through a Streamlit dashboard.

---

## Project structure

```
hockeyproj/
├── Dashboard.py                  ← Streamlit entry point
├── pages/
│   ├── 1_Overview.py
│   ├── 2_Game_Results.py
│   ├── 3_Player_Stats.py
│   ├── 4_Goalie_Stats.py
│   └── 5_Cross_League.py
├── utils/
│   └── data.py                   ← shared loader, league config, filters
├── scrapers/
│   ├── austria/
│   │   ├── austria_pbp_scrapper.py
│   │   └── data/input/           ← games.csv, events.csv, players.csv
│   ├── czech/
│   │   ├── czech_scrapper.py
│   │   └── data/input/           ← games.csv, events.csv, players.csv, goalies.csv
│   ├── finland/
│   │   ├── finland_scrapper.py
│   │   └── data/input/           ← games.csv, events.csv, players.csv, shotmap.csv
│   ├── germany/
│   │   ├── germany_scrapper.py
│   │   └── data/input/           ← games.csv, events.csv, players.csv, goalies.csv
│   ├── slovakia/
│   │   ├── hockeyslovakia_scraper.py
│   │   ├── hockeyslovakia_pbp.py
│   │   ├── slovakia_ml.py
│   │   └── data/input/
│   ├── sweden/
│   │   ├── sweden_scrapper.py
│   │   └── data/input/           ← games.csv, events.csv, players.csv, lineups.csv
│   └── switzerland/
│       ├── switzerland_scrapper.py
│       └── data/input/           ← games.csv, events.csv, players.csv
└── notebooks/
```

---

## Setup

```bash
pip install streamlit pandas plotly requests beautifulsoup4
```

---

## Scrapers

### 🇦🇹 Austria — ICE Hockey League

**Source:** S3 JSON API (`icehl.hokejovyzapis.cz`) — no Playwright needed

```bash
python scrapers/austria/austria_pbp_scrapper.py
```

Outputs to `scrapers/austria/data/input/`: `games.csv`, `events.csv`, `players.csv`

---

### 🇨🇿 Czech Republic — Tipsport Extraliga

**Source:** `json.esports.cz` scoreboard JSON + `hokej.cz` game pages (static HTML) — no Playwright needed

```bash
# Current season (2025-26)
python scrapers/czech/czech_scrapper.py

# Prior season
python scrapers/czech/czech_scrapper.py --season 2024-25

# Single game
python scrapers/czech/czech_scrapper.py --game 2921796
```

Outputs to `scrapers/czech/data/input/`: `games.csv`, `events.csv`, `players.csv`, `goalies.csv`

---

### 🇫🇮 Finland — Liiga

**Source:** liiga.fi JSON API v2 — no Playwright needed

```bash
# Current season regular season
python scrapers/finland/finland_scrapper.py season 2025-26

# Playoffs only
python scrapers/finland/finland_scrapper.py playoffs 2024-25

# Both regular season + playoffs
python scrapers/finland/finland_scrapper.py both 2024-25

# Multiple seasons at once
python scrapers/finland/finland_scrapper.py seasons 2022-23 2023-24 2024-25
```

> **Note:** Season year = the latter of the two years (e.g. 2025-26 → API year 2026)

Outputs to `scrapers/finland/data/input/`: `games.csv`, `events.csv`, `players.csv`, `penalties.csv`, `shotmap.csv`

---

### 🇩🇪 Germany — DEL

**Source:** penny-del.org (static HTML)

```bash
# Current season
python scrapers/germany/germany_scrapper.py --season 2025-26

# Regular season only / playoffs only
python scrapers/germany/germany_scrapper.py --season 2025-26 --type hauptrunde
python scrapers/germany/germany_scrapper.py --season 2025-26 --type playoffs
```

Outputs to `scrapers/germany/data/input/`: `games.csv`, `events.csv`, `players.csv`, `goalies.csv`

---

### 🇸🇪 Sweden — SHL

**Source:** stats.swehockey.se (static HTML)

```bash
# Current season (2025-26) — regular season + playoffs (default)
python scrapers/sweden/sweden_scrapper.py

# Explicit season IDs (regular season + playoffs)
python scrapers/sweden/sweden_scrapper.py --season-id 18263 19791

# Multiple seasons at once
python scrapers/sweden/sweden_scrapper.py --season-id 18263 19791 17556 18507
```

> **Note:** Each SHL season has two IDs — one for the regular season (Grundserie) and one for
> the playoffs (SM-slutspel). Pass both to get the full season.

**Known season IDs:**

| Season  | Regular season (Grundserie) | Playoffs (SM-slutspel) |
|---------|----------------------------|------------------------|
| 2025-26 | 18263                      | 19791                  |
| 2024-25 | 17556                      | 18507                  |

Outputs to `scrapers/sweden/data/input/`: `games.csv`, `events.csv`, `players.csv`, `lineups.csv`, `reports.csv`

---

### 🇨🇭 Switzerland — National League

**Source:** Azure REST API (`app-nationalleague-prod-001.azurewebsites.net`) — no Playwright needed

```bash
# Current season (2025-26)
python scrapers/switzerland/switzerland_scrapper.py

# Prior season
python scrapers/switzerland/switzerland_scrapper.py --season 2024-25

# Single game
python scrapers/switzerland/switzerland_scrapper.py --game 20261105000001
```

> **Note:** gameId prefix = the end year of the season (e.g. `2026` for 2025-26).

Outputs to `scrapers/switzerland/data/input/`: `games.csv`, `events.csv`, `players.csv`

---

### 🇷🇺 KHL — Kontinental Hockey League

**Source:** khl.ru (static HTML, session cookies required)
**Stage IDs:** 1369 = regular season 2025-26, 1370 = playoffs 2025-26

```bash
# Regular season + playoffs
python scrapers/khl/khl_scrapper.py

# Regular season only
python scrapers/khl/khl_scrapper.py --stage regular

# Playoffs only
python scrapers/khl/khl_scrapper.py --stage playoffs

# Single game
python scrapers/khl/khl_scrapper.py --game 1369/897491
```

Outputs to `scrapers/khl/data/input/`: `games.csv`, `events.csv`, `players.csv`, `goalies.csv`

---

### 🇸🇰 Slovakia — Tipsport Liga (Play-by-Play)

**Source:** hockeyslovakia.sk (static HTML)
**Season ID:** 1131 = 2025-26 season

```bash
# Full season play-by-play
python scrapers/slovakia/hockeyslovakia_pbp.py --season-pbp

# Limit to first N matches (useful for testing)
python scrapers/slovakia/hockeyslovakia_pbp.py --season-pbp --limit 10

# Scrape a single match and print JSON
python scrapers/slovakia/hockeyslovakia_pbp.py --match 153021
```

Outputs to `scrapers/slovakia/data/input/`: `games.csv`, `events.csv`, `players.csv`

---

## Dashboard

### Run locally

```bash
cd ~/hockeyproj
python -m streamlit run Dashboard.py
```

Opens at `http://localhost:8501`. The dashboard auto-detects whichever leagues have CSV data in their `data/input/` folders — no configuration needed.

### Pages

| Page | Contents |
|---|---|
| Overview | Key metrics, goals per league, top scorers, recent results |
| Game Results | Searchable results table, goals distribution, home/away win rate |
| Player Stats | Aggregated leaderboard, goals vs assists scatter, points distribution |
| Goalie Stats | Save % rankings, GAA vs SV% scatter, distribution by league |
| Cross-League | League comparison table, monthly scoring trend, penalty heat, player nationality |

### Deploy to Streamlit Community Cloud (free public URL)

```bash
git init
git add .
git commit -m "initial commit"
gh repo create euro-hockey-hub --public --source=. --push
```

Then go to **share.streamlit.io** → sign in with GitHub → New app → select your repo → `Dashboard.py` → Deploy.

---

## League colors

| League | Abbr | Color |
|--------|------|-------|
| ICE Hockey League (Austria) | ICE | `#c77dff` |
| Czech Extraliga | CZE | `#d62828` |
| Liiga (Finland) | FIN | `#57cc6b` |
| DEL (Germany) | DEL | `#ffd166` |
| SHL (Sweden) | SHL | `#4fc3f7` |
| National League (Switzerland) | NL | `#ff6b6b` |
| Tipsport Liga (Slovakia) | SVK | `#9999ff` |
| KHL | KHL | `#e63946` |
