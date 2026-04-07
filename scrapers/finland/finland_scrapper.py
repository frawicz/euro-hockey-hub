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
    liiga_games.csv            — one row per game (metadata + team stats)
    liiga_players.csv          — one row per player (season aggregate)
    liiga_players_game_log.csv — one row per player per game
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


def to_int(value, default=0):
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def to_float(value, default=0.0):
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def normalize_liiga_id(value):
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split(":", 1)[0]


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

    game = data.get("game", data)
    penalties = []
    players = []

    home_team = game.get("homeTeam", {}) or {}
    away_team = game.get("awayTeam", {}) or {}

    # Penalties: current live schema keeps these under each team node.
    penalty_sources = []
    if home_team.get("penaltyEvents"):
        penalty_sources.extend([(ev, home_team.get("teamName", "")) for ev in home_team.get("penaltyEvents", [])])
    if away_team.get("penaltyEvents"):
        penalty_sources.extend([(ev, away_team.get("teamName", "")) for ev in away_team.get("penaltyEvents", [])])

    # Backward compatibility for older payloads.
    if not penalty_sources and data.get("penalties"):
        penalty_sources.extend([(ev, ev.get("teamName", "")) for ev in data.get("penalties", [])])

    for p, team_name in penalty_sources:
        penalized = p.get("penalizedPlayer", {}) or p.get("player", {}) or {}
        served = p.get("servedByPlayer", {}) or {}
        penalties.append({
            "game_id":          game_id,
            "season_year":      year,
            "event_id":         p.get("eventId"),
            "period":           p.get("period"),
            "game_time_s":      p.get("gameTime"),
            "team":             team_name or p.get("teamName", ""),
            "penalized_id":     p.get("playerId", "") or penalized.get("playerId", ""),
            "penalized_name":   player_name(penalized) or "",
            "served_by_id":     served.get("playerId", ""),
            "served_by_name":   player_name(served),
            "penalty_type":     p.get("penaltyName", "") or p.get("penaltyFaultName", ""),
            "penalty_code":     p.get("penaltyFaultType", ""),
            "minutes":          p.get("penaltyMinutes"),
            "is_bench":         p.get("isBench", False),
        })

    def extract_player_rows(raw_players, team_data, side_name):
        rows = []
        team_name = team_data.get("teamName", "")
        team_id = team_data.get("teamId", "")
        for p in raw_players:
            player = p.get("player", {}) or p
            stats = p.get("gameStats", {}) or {}
            name = (
                player_name(player)
                or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
            )
            rows.append({
                "game_id":       game_id,
                "season_year":   year,
                "team_side":     side_name,
                "team":          team_name,
                "team_id":       team_id or p.get("teamId", ""),
                "player_id":     player.get("playerId", "") or p.get("id", ""),
                "name":          name,
                "first_name":    player.get("firstName", "") or p.get("firstName", ""),
                "last_name":     player.get("lastName", "") or p.get("lastName", ""),
                "jersey":        p.get("jerseyNumber", "") or p.get("jersey", ""),
                "position":      player.get("position", "") or p.get("roleCode", "") or p.get("role", ""),
                "nationality":   player.get("nationality", "") or p.get("nationality", ""),
                "birth_date":    player.get("birthDate", "") or p.get("dateOfBirth", ""),
                "height_cm":     player.get("height", "") or p.get("height", ""),
                "weight_kg":     player.get("weight", "") or p.get("weight", ""),
                "catches":       player.get("catches", ""),
                "shoots":        player.get("shoots", "") or p.get("handedness", ""),
                "line_number":   p.get("lineNumber", "") or p.get("line", ""),
                "is_captain":    p.get("isCaptain", False) or p.get("captain", False),
                "is_alternate":  p.get("isAlternate", False) or p.get("alternateCaptain", False),
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
                "pp_goals":      stats.get("powerplayGoals", ""),
                "pp_assists":    stats.get("powerplayAssists", ""),
                "sh_goals":      stats.get("shortHandedGoals", ""),
                "sh_assists":    stats.get("shortHandedAssists", ""),
                # Goalie stats
                "saves":         stats.get("saves", ""),
                "goals_against": stats.get("goalsAgainst", ""),
                "save_pct":      stats.get("savePercentage", ""),
            })
        return rows

    # Current live schema.
    home_players_live = data.get("homeTeamPlayers", []) or game.get("homeTeamPlayers", [])
    away_players_live = data.get("awayTeamPlayers", []) or game.get("awayTeamPlayers", [])
    if home_players_live or away_players_live:
        players.extend(extract_player_rows(home_players_live, home_team, "home"))
        players.extend(extract_player_rows(away_players_live, away_team, "away"))

    # Backward compatibility for the older nested roster structure.
    if not players:
        for side_key, side_name in [("homeTeam", "home"), ("awayTeam", "away")]:
            team_data = data.get(side_key, {}) or game.get(side_key, {}) or {}
            raw_players = team_data.get("players", [])
            players.extend(extract_player_rows(raw_players, team_data, side_name))

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

def fetch_shotmap(game_id, year, game_row=None, players=None):
    url = f"{BASE}/shotmap/{year}/{game_id}"
    data = fetch_json(url)
    if not data:
        return []

    game_row = game_row or {}
    players = players or []
    team_by_id = {}
    side_by_id = {}
    player_name_by_id = {}

    home_team_id = normalize_liiga_id(game_row.get("home_team_id", ""))
    away_team_id = normalize_liiga_id(game_row.get("away_team_id", ""))
    if home_team_id:
        team_by_id[home_team_id] = game_row.get("home_team", "")
        side_by_id[home_team_id] = "home"
    if away_team_id:
        team_by_id[away_team_id] = game_row.get("away_team", "")
        side_by_id[away_team_id] = "away"

    for p in players:
        pid = str(p.get("player_id", "")).strip()
        if pid and p.get("name"):
            player_name_by_id[pid] = p.get("name")
        tid = normalize_liiga_id(p.get("team_id", ""))
        if tid and p.get("team"):
            team_by_id.setdefault(tid, p.get("team"))
            side_by_id.setdefault(tid, p.get("team_side"))

    shots = []
    all_shots = data if isinstance(data, list) else data.get("shots", [])
    for s in all_shots:
        shooter_id = str(s.get("shooterId", "")).strip()
        shooting_team_id = str(s.get("shootingTeamId", "")).strip()
        shot_kind = s.get("type", "") or ""
        event_type = s.get("eventType", "") or ""
        own_skaters = s.get("ownTeamPlayersOnIce", "")
        opp_skaters = s.get("otherTeamPlayersOnIce", "")
        strength_state = ""
        if own_skaters != "" and opp_skaters != "":
            strength_state = f"{own_skaters}v{opp_skaters}"

        shots.append({
            "game_id":      game_id,
            "season_year":  year,
            "period":       s.get("period"),
            "game_time_s":  s.get("gameTime"),
            "team_id":      shooting_team_id,
            "team":         team_by_id.get(shooting_team_id, ""),
            "team_side":    side_by_id.get(shooting_team_id, ""),
            "player_id":    shooter_id,
            "player_name":  player_name_by_id.get(shooter_id, ""),
            "result":       event_type,
            "shot_type":    shot_kind,
            "event_type":   event_type,
            "strength_state": strength_state,
            "own_skaters":  own_skaters,
            "opp_skaters":  opp_skaters,
            "blocker_id":   s.get("blockerId", ""),
            "left_team_id": s.get("leftTeam", ""),
            "right_team_id":s.get("rightTeam", ""),
            "x":            s.get("shotX", ""),
            "y":            s.get("shotY", ""),
            "goal_types":   ",".join(s.get("goalTypes", [])) if s.get("goalTypes") else "",
        })
    return shots


def aggregate_players(players, goals, shots, penalties=None):
    """
    Collapse per-game Finland player rows into a season-level player table and
    enrich it with event-derived scoring splits.
    """
    if not players:
        return []

    goal_lookup = {}
    assist1_lookup = {}
    assist2_lookup = {}
    gwg_lookup = {}
    pp_goal_lookup = {}
    sh_goal_lookup = {}
    ev_goal_lookup = {}
    penalty_lookup = {}

    for ev in goals:
        gid = str(ev.get("game_id", ""))
        scorer_id = str(ev.get("scorer_id", "")).strip()
        assist1_id = str(ev.get("assist1_id", "")).strip()
        assist2_id = str(ev.get("assist2_id", "")).strip()
        goal_types = str(ev.get("goal_types", "")).upper()
        winner = bool(ev.get("winning_goal", False))

        if scorer_id:
            key = (gid, scorer_id)
            goal_lookup[key] = goal_lookup.get(key, 0) + 1
            if "YV" in goal_types or "PP" in goal_types:
                pp_goal_lookup[key] = pp_goal_lookup.get(key, 0) + 1
            elif "AV" in goal_types or "SH" in goal_types:
                sh_goal_lookup[key] = sh_goal_lookup.get(key, 0) + 1
            else:
                ev_goal_lookup[key] = ev_goal_lookup.get(key, 0) + 1
            if winner:
                gwg_lookup[key] = gwg_lookup.get(key, 0) + 1

        if assist1_id:
            key = (gid, assist1_id)
            assist1_lookup[key] = assist1_lookup.get(key, 0) + 1
        if assist2_id:
            key = (gid, assist2_id)
            assist2_lookup[key] = assist2_lookup.get(key, 0) + 1

    shot_lookup = {}
    for sh in shots:
        gid = str(sh.get("game_id", ""))
        pid = str(sh.get("player_id", "")).strip()
        if not pid:
            continue
        key = (gid, pid)
        shot_lookup[key] = shot_lookup.get(key, 0) + 1

    penalties = penalties or []
    for pen in penalties:
        gid = str(pen.get("game_id", ""))
        pid = str(pen.get("penalized_id", "")).strip()
        if not pid:
            continue
        key = (gid, pid)
        penalty_lookup[key] = penalty_lookup.get(key, 0) + to_int(pen.get("minutes", 0))

    grouped = {}
    for row in players:
        pid = str(row.get("player_id", "")).strip()
        name = str(row.get("name", "")).strip() or f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        if not pid and not name:
            continue

        key = (
            str(row.get("season_year", "")),
            str(row.get("team", "")),
            pid or name,
        )
        gid = str(row.get("game_id", ""))
        game_player_key = (gid, pid)

        current = grouped.setdefault(
            key,
            {
                "season_year": row.get("season_year", ""),
                "team": row.get("team", ""),
                "team_side": row.get("team_side", ""),
                "player_id": pid,
                "name": name,
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "position": row.get("position", ""),
                "nationality": row.get("nationality", ""),
                "birth_date": row.get("birth_date", ""),
                "height_cm": row.get("height_cm", ""),
                "weight_kg": row.get("weight_kg", ""),
                "shoots": row.get("shoots", ""),
                "catches": row.get("catches", ""),
                "games": 0,
                "goals": 0,
                "assists": 0,
                "points": 0,
                "goals_event": 0,
                "assists_event": 0,
                "points_event": 0,
                "plus_minus": 0,
                "pim": 0,
                "pim_event": 0,
                "shots": 0,
                "shots_from_shotmap": 0,
                "ice_time_s": 0,
                "faceoffs_won": 0,
                "faceoffs_total": 0,
                "pp_goals": 0,
                "pp_assists": 0,
                "sh_goals": 0,
                "sh_assists": 0,
                "primary_assists": 0,
                "secondary_assists": 0,
                "even_strength_goals": 0,
                "powerplay_goals_event": 0,
                "shorthanded_goals_event": 0,
                "game_winning_goals": 0,
                "saves": 0,
                "goals_against": 0,
                "is_goalie": False,
            },
        )

        current["games"] += 1
        for field in [
            "goals",
            "assists",
            "points",
            "plus_minus",
            "pim",
            "shots",
            "ice_time_s",
            "faceoffs_won",
            "faceoffs_total",
            "pp_goals",
            "pp_assists",
            "sh_goals",
            "sh_assists",
            "saves",
            "goals_against",
        ]:
            current[field] += to_int(row.get(field, 0))

        current["shots_from_shotmap"] += shot_lookup.get(game_player_key, 0)
        current["primary_assists"] += assist1_lookup.get(game_player_key, 0)
        current["secondary_assists"] += assist2_lookup.get(game_player_key, 0)
        current["even_strength_goals"] += ev_goal_lookup.get(game_player_key, 0)
        current["powerplay_goals_event"] += pp_goal_lookup.get(game_player_key, 0)
        current["shorthanded_goals_event"] += sh_goal_lookup.get(game_player_key, 0)
        current["game_winning_goals"] += gwg_lookup.get(game_player_key, 0)
        current["goals_event"] += goal_lookup.get(game_player_key, 0)
        current["assists_event"] += assist1_lookup.get(game_player_key, 0) + assist2_lookup.get(game_player_key, 0)
        current["pim_event"] += penalty_lookup.get(game_player_key, 0)
        current["is_goalie"] = current["is_goalie"] or str(row.get("position", "")).upper() in {"G", "GK", "GOALIE"}

    out = []
    for row in grouped.values():
        # Current live Liiga player payload is rich on identity but may not
        # include per-game boxscore stats. Fall back to event-derived counts.
        if row["goals"] == 0 and row["goals_event"] > 0:
            row["goals"] = row["goals_event"]
        if row["assists"] == 0 and row["assists_event"] > 0:
            row["assists"] = row["assists_event"]
        if row["points"] == 0 and (row["goals"] or row["assists"]):
            row["points"] = row["goals"] + row["assists"]
        row["points_event"] = row["goals_event"] + row["assists_event"]
        if row["shots"] == 0 and row["shots_from_shotmap"] > 0:
            row["shots"] = row["shots_from_shotmap"]
        if row["pim"] == 0 and row["pim_event"] > 0:
            row["pim"] = row["pim_event"]

        gp = max(int(row["games"]), 1)
        row["goals_per_game"] = round(row["goals"] / gp, 3)
        row["assists_per_game"] = round(row["assists"] / gp, 3)
        row["points_per_game"] = round(row["points"] / gp, 3)
        row["shots_per_game"] = round(row["shots"] / gp, 3)
        row["ice_time_min_per_game"] = round(row["ice_time_s"] / gp / 60.0, 2) if row["ice_time_s"] else 0.0
        row["faceoff_pct"] = round(100.0 * row["faceoffs_won"] / row["faceoffs_total"], 2) if row["faceoffs_total"] else ""
        row["shooting_pct"] = round(100.0 * row["goals"] / row["shots"], 2) if row["shots"] else ""
        row["save_pct_calc"] = round(100.0 * row["saves"] / (row["saves"] + row["goals_against"]), 2) if (row["saves"] + row["goals_against"]) else ""
        out.append(row)

    out.sort(key=lambda r: (r.get("season_year", ""), r.get("points", 0), r.get("goals", 0)), reverse=True)
    return out


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
        shots = fetch_shotmap(gid, year, game_row=game_row, players=players)
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
    players_agg = aggregate_players(all_players, all_goals, all_shots, all_penalties)

    write_csv(all_games,     out_dir / "games.csv")
    write_csv(players_agg,   out_dir / "players.csv")
    write_csv(all_players,   out_dir / "players_game_log.csv")
    write_csv(all_goals,     out_dir / "events.csv")
    write_csv(all_penalties, out_dir / "penalties.csv")
    write_csv(all_shots,     out_dir / "shotmap.csv")

    print(f"\nDone!")
    print(f"  Games     : {len(all_games):,}")
    print(f"  Players   : {len(players_agg):,} aggregated / {len(all_players):,} game logs")
    print(f"  Goals     : {len(all_goals):,}")
    print(f"  Penalties : {len(all_penalties):,}")
    print(f"  Shots     : {len(all_shots):,}")


if __name__ == "__main__":
    main()
