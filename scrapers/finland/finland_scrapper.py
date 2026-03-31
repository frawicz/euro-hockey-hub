#!/usr/bin/env python3
"""
Liiga (Finnish Hockey League) — Season Stats Scraper  [API v2 edition]
=======================================================================
Uses liiga.fi's JSON API directly — no HTML parsing, no Playwright needed.

API endpoints:
  Games list : https://liiga.fi/api/v2/games?tournament={tournament}&season={year}
  Game detail: https://liiga.fi/api/v2/games/{year}/{id}
  Game stats : https://liiga.fi/api/v2/games/stats/{year}/{id}
  Shot map   : https://liiga.fi/api/v2/shotmap/{year}/{id}

Season year = the latter of the two years (e.g. 2025-26 season → year=2026)
Tournaments : runkosarja | playoffs | chl | valmistavat_ottelut

Requirements:
    pip install requests pandas

Usage:
    python liiga_scraper.py season 2025-26
    python liiga_scraper.py playoffs 2024-25
    python liiga_scraper.py both 2024-25
    python liiga_scraper.py seasons 2022-23 2023-24 2024-25

Outputs:
    liiga_games.csv      — one row per game (metadata + team stats)
    liiga_players.csv    — one row per player per game
    liiga_events.csv     — one row per goal event
    liiga_penalties.csv  — one row per penalty event
    liiga_shotmap.csv    — one row per shot
"""

import sys
import time
import csv
import json
from pathlib import Path

try:
    import requests
except ImportError:
    print("Missing dep. Run:  pip install requests")
    sys.exit(1)

BASE = "https://liiga.fi/api/v2"
DELAY = 0.3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://liiga.fi/",
}

TOURNAMENT_MAP = {
    "runkosarja":           "runkosarja",
    "regular":              "runkosarja",
    "season":               "runkosarja",
    "playoffs":             "playoffs",
    "playoff":              "playoffs",
    "chl":                  "chl",
    "valmistavat_ottelut":  "valmistavat_ottelut",
    "preseason":            "valmistavat_ottelut",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                print(f"    [!] Failed: {url} — {e}")
                return None
            time.sleep(1.5)


# ── Season year helper ────────────────────────────────────────────────────────

def season_to_year(season_str):
    """
    '2025-26' → 2026
    '2024-25' → 2025
    '2022-2023' → 2023
    """
    parts = season_str.replace("–", "-").split("-")
    last = parts[-1].strip()
    if len(last) == 2:
        prefix = parts[0][:2]
        return int(prefix + last)
    return int(last)


# ── Flat helpers ──────────────────────────────────────────────────────────────

def safe(d, *keys, default=""):
    """Safely navigate nested dict/list."""
    val = d
    for k in keys:
        if val is None:
            return default
        if isinstance(val, dict):
            val = val.get(k)
        elif isinstance(val, list) and isinstance(k, int):
            val = val[k] if k < len(val) else None
        else:
            return default
    return val if val is not None else default


def player_name(p):
    if not p:
        return ""
    return f"{p.get('firstName','')} {p.get('lastName','')}".strip()


# ── Games list ────────────────────────────────────────────────────────────────

def get_games_list(year, tournament="runkosarja"):
    url = f"{BASE}/games?tournament={tournament}&season={year}"
    data = fetch_json(url)
    if data is None:
        return []
    if isinstance(data, dict):
        # sometimes wrapped in an object
        return data.get("games", data.get("data", []))
    return data


# ── Parse a single game from the list response ────────────────────────────────

def parse_game_row(g, year, tournament):
    ht = g.get("homeTeam", {})
    at = g.get("awayTeam", {})
    rink = g.get("iceRink", {})

    row = {
        "game_id":           g.get("id"),
        "season_year":       year,
        "tournament":        tournament,
        "game_week":         g.get("gameWeek"),
        "date":              g.get("gameStartDateTime", "")[:10],
        "start_utc":         g.get("start", ""),
        "end_utc":           g.get("end", ""),
        "home_team":         ht.get("teamName", ""),
        "away_team":         at.get("teamName", ""),
        "home_team_id":      ht.get("teamId", ""),
        "away_team_id":      at.get("teamId", ""),
        "home_goals":        ht.get("goals", ""),
        "away_goals":        at.get("goals", ""),
        "home_pp_instances": ht.get("powerplayInstances", ""),
        "home_pp_goals":     ht.get("powerplayGoals", ""),
        "home_sh_instances": ht.get("shortHandedInstances", ""),
        "home_sh_goals":     ht.get("shortHandedGoals", ""),
        "home_xg":           ht.get("expectedGoals", ""),
        "away_pp_instances": at.get("powerplayInstances", ""),
        "away_pp_goals":     at.get("powerplayGoals", ""),
        "away_sh_instances": at.get("shortHandedInstances", ""),
        "away_sh_goals":     at.get("shortHandedGoals", ""),
        "away_xg":           at.get("expectedGoals", ""),
        "finished_type":     g.get("finishedType", ""),
        "game_time_s":       g.get("gameTime", ""),
        "spectators":        g.get("spectators", ""),
        "current_period":    g.get("currentPeriod", ""),
        "started":           g.get("started", ""),
        "ended":             g.get("ended", ""),
        "arena":             rink.get("name", ""),
        "city":              rink.get("city", ""),
        "arena_lat":         rink.get("latitude", ""),
        "arena_lon":         rink.get("longitude", ""),
        "buy_tickets_url":   g.get("buyTicketsUrl", ""),
    }
    return row


# ── Parse goal events from the list response ──────────────────────────────────

def parse_goal_events(g, year, tournament):
    gid = g.get("id")
    ht = g.get("homeTeam", {})
    at = g.get("awayTeam", {})
    events = []

    for team_side, team_data in [("home", ht), ("away", at)]:
        for ev in team_data.get("goalEvents", []):
            scorer = ev.get("scorerPlayer")
            asst = ev.get("assistantPlayers", [])
            events.append({
                "game_id":          gid,
                "season_year":      year,
                "tournament":       tournament,
                "event_id":         ev.get("eventId"),
                "period":           ev.get("period"),
                "game_time_s":      ev.get("gameTime"),
                "log_time":         ev.get("logTime", ""),
                "team_side":        team_side,
                "team":             team_data.get("teamName", ""),
                "scorer_id":        ev.get("scorerPlayerId"),
                "scorer_name":      player_name(scorer),
                "goal_types":       ",".join(ev.get("goalTypes", [])),
                "winning_goal":     ev.get("winningGoal", False),
                "home_score":       ev.get("homeTeamScore"),
                "away_score":       ev.get("awayTeamScore"),
                "assist1_id":       safe(asst, 0, "playerId"),
                "assist1_name":     player_name(safe(asst, 0)),
                "assist2_id":       safe(asst, 1, "playerId"),
                "assist2_name":     player_name(safe(asst, 1)),
                "plus_players":     ev.get("plusPlayerIds", ""),
                "minus_players":    ev.get("minusPlayerIds", ""),
                "goals_in_season":  ev.get("goalsSoFarInSeason"),
            })
    return events


# ── Game detail endpoint: penalties + player shifts ──────────────────────────

def fetch_game_detail(game_id, year):
    """
    Returns (penalties_list, players_list) from /api/v2/games/{year}/{id}
    """
    url = f"{BASE}/games/{year}/{game_id}"
    data = fetch_json(url)
    if not data:
        return [], []

    penalties = []
    players = []

    # Penalties
    for p in data.get("penalties", []):
        penalized = p.get("penalizedPlayer", {}) or {}
        served = p.get("servedByPlayer", {}) or {}
        penalties.append({
            "game_id":          game_id,
            "season_year":      year,
            "event_id":         p.get("eventId"),
            "period":           p.get("period"),
            "game_time_s":      p.get("gameTime"),
            "team":             p.get("teamName", ""),
            "penalized_id":     penalized.get("playerId", ""),
            "penalized_name":   player_name(penalized),
            "served_by_id":     served.get("playerId", ""),
            "served_by_name":   player_name(served),
            "penalty_type":     p.get("penaltyName", ""),
            "minutes":          p.get("penaltyMinutes"),
            "is_bench":         p.get("isBench", False),
        })

    # Home / Away rosters
    for side_key, side_name in [("homeTeam", "home"), ("awayTeam", "away")]:
        team_data = data.get(side_key, {})
        team_name = team_data.get("teamName", "")
        for p in team_data.get("players", []):
            player = p.get("player", {}) or {}
            stats = p.get("gameStats", {}) or {}
            players.append({
                "game_id":       game_id,
                "season_year":   year,
                "team_side":     side_name,
                "team":          team_name,
                "player_id":     player.get("playerId", ""),
                "first_name":    player.get("firstName", ""),
                "last_name":     player.get("lastName", ""),
                "jersey":        p.get("jerseyNumber", ""),
                "position":      player.get("position", ""),
                "nationality":   player.get("nationality", ""),
                "line_number":   p.get("lineNumber", ""),
                "is_captain":    p.get("isCaptain", False),
                "is_alternate":  p.get("isAlternate", False),
                "is_starting_gk":p.get("isStartingGoalie", False),
                # Skater stats
                "goals":         stats.get("goals", ""),
                "assists":       stats.get("assists", ""),
                "points":        stats.get("points", ""),
                "plus_minus":    stats.get("plusMinus", ""),
                "pim":           stats.get("penaltyMinutes", ""),
                "shots":         stats.get("shots", ""),
                "ice_time_s":    stats.get("iceTime", ""),
                "faceoffs_won":  stats.get("faceoffsWon", ""),
                "faceoffs_total":stats.get("faceoffs", ""),
                # Goalie stats
                "saves":         stats.get("saves", ""),
                "goals_against": stats.get("goalsAgainst", ""),
                "save_pct":      stats.get("savePercentage", ""),
            })

    return penalties, players


# ── Game stats endpoint: xG, shots by period, etc. ───────────────────────────

def fetch_game_stats(game_id, year):
    """Merges shot/period data into the game row — returns a dict or {}."""
    url = f"{BASE}/games/stats/{year}/{game_id}"
    data = fetch_json(url)
    if not data:
        return {}

    extra = {}
    # Period-by-period shots
    for team_key, prefix in [("homeTeamStats", "home"), ("awayTeamStats", "away")]:
        team = data.get(team_key, {}) or {}
        extra[f"{prefix}_shots_total"]   = team.get("shotsTotal", "")
        extra[f"{prefix}_shots_on_goal"] = team.get("shotsOnGoal", "")
        extra[f"{prefix}_blocked_shots"] = team.get("blockedShots", "")
        extra[f"{prefix}_missed_shots"]  = team.get("missedShots", "")
        extra[f"{prefix}_faceoffs_won"]  = team.get("faceoffsWon", "")
        extra[f"{prefix}_faceoffs_total"]= team.get("faceoffs", "")
    return extra


# ── Shot map ─────────────────────────────────────────────────────────────────

def fetch_shotmap(game_id, year):
    url = f"{BASE}/shotmap/{year}/{game_id}"
    data = fetch_json(url)
    if not data:
        return []

    shots = []
    all_shots = data if isinstance(data, list) else data.get("shots", [])
    for s in all_shots:
        shooter = s.get("player", {}) or {}
        shots.append({
            "game_id":      game_id,
            "season_year":  year,
            "period":       s.get("period"),
            "game_time_s":  s.get("gameTime"),
            "team":         s.get("teamName", ""),
            "team_side":    s.get("teamSide", ""),
            "player_id":    shooter.get("playerId", ""),
            "player_name":  player_name(shooter),
            "result":       s.get("result", ""),
            "shot_type":    s.get("shotType", ""),
            "x":            s.get("x", ""),
            "y":            s.get("y", ""),
            "goal_types":   ",".join(s.get("goalTypes", [])) if s.get("goalTypes") else "",
        })
    return shots


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        print(f"  (empty — skipping {path})")
        return
    keys = list({k for row in rows for k in row})
    priority = ["game_id", "season_year", "tournament", "date", "home_team",
                "away_team", "home_goals", "away_goals", "player_id",
                "first_name", "last_name", "team", "period", "game_time_s"]
    cols = [c for c in priority if c in keys] + sorted(c for c in keys if c not in priority)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):>6,} rows → {path}")


# ── Main scrape loop ──────────────────────────────────────────────────────────

def scrape(season_str, tournament_key="runkosarja", output_dir="."):
    year = season_to_year(season_str)
    tournament = TOURNAMENT_MAP.get(tournament_key.lower(), tournament_key)

    print(f"\n{'─'*60}")
    print(f"Scraping  season={season_str} (year={year})  tournament={tournament}")

    games_list = get_games_list(year, tournament)
    # Filter to finished games only
    finished = [g for g in games_list if g.get("ended", False)]
    print(f"  {len(games_list)} total games, {len(finished)} finished.")

    all_games, all_goals, all_penalties, all_players, all_shots = [], [], [], [], []

    for i, g in enumerate(finished, 1):
        gid = g["id"]
        ht = g.get("homeTeam", {}).get("teamName", "?")
        at = g.get("awayTeam", {}).get("teamName", "?")
        hg = g.get("homeTeam", {}).get("goals", "?")
        ag = g.get("awayTeam", {}).get("goals", "?")
        print(f"  [{i:3d}/{len(finished)}] #{gid:5d}  {ht} {hg}-{ag} {at} … ", end="", flush=True)

        # Game row from list
        game_row = parse_game_row(g, year, tournament)

        # Goal events from list (already embedded)
        goals = parse_goal_events(g, year, tournament)

        # Game detail: penalties + players
        penalties, players = fetch_game_detail(gid, year)
        time.sleep(DELAY)

        # Extra shot stats
        extra_stats = fetch_game_stats(gid, year)
        game_row.update(extra_stats)
        time.sleep(DELAY)

        # Shot map
        shots = fetch_shotmap(gid, year)
        time.sleep(DELAY)

        all_games.append(game_row)
        all_goals.extend(goals)
        all_penalties.extend(penalties)
        all_players.extend(players)
        all_shots.extend(shots)

        print(f"OK  ({len(goals)}g  {len(penalties)}pen  {len(players)}pl  {len(shots)}sh)")

    return all_games, all_goals, all_penalties, all_players, all_shots


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        print("\nExample:  python liiga_scraper.py season 2024-25")
        sys.exit(0)

    mode = args[0].lower()
    seasons = args[1:] if len(args) > 1 else ["2024-25"]

    out_dir = Path(__file__).parent / "data" / "input"

    tasks = []
    if mode in ("season", "regular"):
        for s in seasons:
            tasks.append((s, "runkosarja"))
    elif mode == "playoffs":
        for s in seasons:
            tasks.append((s, "playoffs"))
    elif mode == "both":
        for s in seasons:
            tasks.append((s, "runkosarja"))
            tasks.append((s, "playoffs"))
    elif mode == "seasons":
        for s in seasons:
            tasks.append((s, "runkosarja"))
            tasks.append((s, "playoffs"))
    elif mode == "preseason":
        for s in seasons:
            tasks.append((s, "valmistavat_ottelut"))
    else:
        # treat mode itself as a season string
        tasks.append((mode, "runkosarja"))
        for s in seasons:
            tasks.append((s, "runkosarja"))

    all_games, all_goals, all_penalties, all_players, all_shots = [], [], [], [], []

    for season_str, tournament in tasks:
        g, go, pen, pl, sh = scrape(season_str, tournament, out_dir)
        all_games.extend(g)
        all_goals.extend(go)
        all_penalties.extend(pen)
        all_players.extend(pl)
        all_shots.extend(sh)

    print(f"\n{'─'*60}")
    print("Writing CSVs …")
    write_csv(all_games,     out_dir / "games.csv")
    write_csv(all_players,   out_dir / "players.csv")
    write_csv(all_goals,     out_dir / "events.csv")
    write_csv(all_penalties, out_dir / "penalties.csv")
    write_csv(all_shots,     out_dir / "shotmap.csv")

    print(f"\nDone!")
    print(f"  Games     : {len(all_games):,}")
    print(f"  Players   : {len(all_players):,}")
    print(f"  Goals     : {len(all_goals):,}")
    print(f"  Penalties : {len(all_penalties):,}")
    print(f"  Shots     : {len(all_shots):,}")


if __name__ == "__main__":
    main()