#!/usr/bin/env python3
"""
ICE Hockey League (ICEHL / win2day ICE Hockey League) — Season Scraper
=======================================================================
Reads directly from the icehl.hokejovyzapis.cz JSON API (no Playwright needed).

Output → austria/data/input/:
  games.csv    — one row per game (metadata, score, period breakdown)
  events.csv   — goals, penalties, goalie changes (one row per event)
  players.csv  — season-aggregated stats per skater/goalie

Usage:
    python austria/austria_pbp_scrapper.py                  # 2025/26 season
    python austria/austria_pbp_scrapper.py --season 2024    # prior season
    python austria/austria_pbp_scrapper.py --game 7832      # single game
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from pathlib import Path
from typing import Any

try:
    import requests
    import pandas as pd
except ImportError:
    print("Missing deps. Run: pip install requests pandas")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SCHEDULE_URL = (
    "https://s3-eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz"
    "/league-matches/{season}/1.json"
)
MATCH_URL = (
    "https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz"
    "/widget/esports/match/{game_id}.json"
)
OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DELAY = 0.3  # seconds between requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3) -> dict | None:
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
    return None


def player_name(p: dict | None) -> str:
    """Format as 'Lastname Firstname' for event participants (goals/penalties/GK).
    In event sections, playerLastname=actual last, playerFirstname=actual first."""
    if not p:
        return ""
    last = (p.get("playerLastname") or "").strip()
    first = (p.get("playerFirstname") or "").strip()
    return f"{last} {first}".strip()


def player_name_stats(p: dict | None) -> str:
    """Format as 'Lastname Firstname' for field-player/goalie stat rows.
    In homeFieldPlayers/awayFieldPlayers the API has the fields swapped:
    playerLastname holds the actual first name, playerFirstname holds the last name."""
    if not p:
        return ""
    # swap: actual last name is in playerFirstname, actual first in playerLastname
    last = (p.get("playerFirstname") or "").strip()
    first = (p.get("playerLastname") or "").strip()
    return f"{last} {first}".strip()


def period_relative_time(elapsed_seconds: int, period: int) -> str:
    """Convert absolute elapsed seconds to MM:SS within-period time."""
    # Regulation: 3 × 20 min = 1200 s each. OT/SO: period 4+.
    period_start = (min(period, 4) - 1) * 1200
    rel = max(0, elapsed_seconds - period_start)
    return f"{rel // 60}:{rel % 60:02d}"


def safe_join(players: list[dict], sep: str = ",") -> str:
    return sep.join(str(p.get("playerJerseyNr", "")) for p in players if p)


# ── Schedule ──────────────────────────────────────────────────────────────────

def get_game_ids(season: int = 2025) -> list[int]:
    """Return IDs of all completed ICEHL games for the given season year."""
    url = SCHEDULE_URL.format(season=season)
    data = fetch_json(url)
    if data is None:
        return []

    matches = data.get("matches", [])
    ids = [
        m["id"]
        for m in matches
        if m.get("status") == "AFTER_MATCH"
        and m.get("advanced_statistics")
    ]
    print(f"  Found {len(ids)} completed games with advanced stats (season {season})")
    return sorted(ids)


# ── Game parser ───────────────────────────────────────────────────────────────

def parse_game_row(data: dict, game_id: int) -> dict[str, Any]:
    """Extract games.csv row from match JSON data."""
    gd = data.get("gameData", {})

    period_scores = [
        f"{ps['homeScore']}:{ps['awayScore']}"
        for ps in gd.get("periodStats", [])
        if ps.get("period")
    ]

    home_stats = data.get("homeTeamStats", {})
    away_stats = data.get("awayTeamStats", {})

    return {
        "game_id": game_id,
        "date": gd.get("scheduledDate", {}).get("value", ""),
        "start_time": gd.get("startTime", ""),
        "home_team": gd.get("homeTeamLongname", ""),
        "away_team": gd.get("awayTeamLongname", ""),
        "home_score": gd.get("homeTeamScore", ""),
        "away_score": gd.get("awayTeamScore", ""),
        "score": f"{gd.get('homeTeamScore','')}:{gd.get('awayTeamScore','')}",
        "period_scores": " | ".join(period_scores),
        "is_overtime": int(gd.get("isOvertime", 0)),
        "is_shootout": int(gd.get("isShootOut", 0)),
        "venue": gd.get("location", {}).get("longname", ""),
        "attendance": gd.get("attendance", ""),
        "division": gd.get("divisionLongname", ""),
        "game_name": gd.get("gameName", ""),
        "round": gd.get("gameDay", ""),
        "home_shots": home_stats.get("shotsOnGoal", ""),
        "away_shots": away_stats.get("shotsOnGoal", ""),
        "home_pim": home_stats.get("penaltyMinutes", ""),
        "away_pim": away_stats.get("penaltyMinutes", ""),
        "home_pp_goals": home_stats.get("powerplayGoals", ""),
        "home_pp_opps": home_stats.get("powerplayOpportunities", ""),
        "away_pp_goals": away_stats.get("powerplayGoals", ""),
        "away_pp_opps": away_stats.get("powerplayOpportunities", ""),
        "url": MATCH_URL.format(game_id=game_id),
    }


# ── Events parser ─────────────────────────────────────────────────────────────

def parse_events(data: dict, game_id: int) -> list[dict[str, Any]]:
    """Build events list from goals, penalties, and GK changes."""
    gd = data.get("gameData", {})
    home_team = gd.get("homeTeamShortname", "HOME")
    away_team = gd.get("awayTeamShortname", "AWAY")
    events: list[dict[str, Any]] = []

    # ── Goals ────────────────────────────────────────────────────────────────
    for team_key, team_name in (
        ("homeGoals", home_team),
        ("awayGoals", away_team),
    ):
        for goal in data.get(team_key, []):
            elapsed = goal.get("gameTime", 0)
            period = goal.get("gameTimePeriod", 1)
            home_ice = goal.get("homePlayersOnIce", [])
            away_ice = goal.get("awayPlayersOnIce", [])
            events.append({
                "game_id": game_id,
                "elapsed_seconds": elapsed,
                "period": period,
                "time": period_relative_time(elapsed, period),
                "event_type": "goal",
                "team": team_name,
                "player": player_name(goal.get("scoredBy")),
                "player_jersey": (goal.get("scoredBy") or {}).get("playerJerseyNr", ""),
                "secondary_player_1": player_name(goal.get("assistBy")),
                "secondary_player_2": player_name(goal.get("assist2By")),
                "score_state": goal.get("newScore", ""),
                "goal_type": goal.get("gameStrength", ""),
                "is_empty_net": int(goal.get("isEmptyNet", False)),
                "is_penalty_shot": int(goal.get("isPenaltyShot", False)),
                "penalty_minutes": "",
                "penalty_reason": "",
                "on_ice_home": safe_join(home_ice),
                "on_ice_away": safe_join(away_ice),
            })

    # ── Penalties ─────────────────────────────────────────────────────────────
    for team_key, team_name in (
        ("homePenalties", home_team),
        ("awayPenalties", away_team),
    ):
        for pen in data.get(team_key, []):
            elapsed = pen.get("gameTime", 0)
            period = pen.get("gameTimePeriod", 1)
            # penaltyLength is in seconds; convert to minutes
            pen_secs = pen.get("penaltyLength", 0)
            pen_mins = pen_secs // 60 if pen_secs else ""
            events.append({
                "game_id": game_id,
                "elapsed_seconds": elapsed,
                "period": period,
                "time": period_relative_time(elapsed, period),
                "event_type": "penalty",
                "team": team_name,
                "player": player_name(pen.get("offender")),
                "player_jersey": (pen.get("offender") or {}).get("playerJerseyNr", ""),
                "secondary_player_1": player_name(pen.get("servedBy")),
                "secondary_player_2": "",
                "score_state": "",
                "goal_type": "",
                "is_empty_net": "",
                "is_penalty_shot": "",
                "penalty_minutes": pen_mins,
                "penalty_reason": pen.get("offence", ""),
                "on_ice_home": "",
                "on_ice_away": "",
            })

    # ── Goalie changes ────────────────────────────────────────────────────────
    for team_key, team_name in (
        ("homeGoalKeeperChanges", home_team),
        ("awayGoalKeeperChanges", away_team),
    ):
        for gc in data.get(team_key, []):
            elapsed = gc.get("gametime", 0)
            period = gc.get("gameTimePeriod", 1)
            action = gc.get("action", "")
            events.append({
                "game_id": game_id,
                "elapsed_seconds": elapsed,
                "period": period,
                "time": period_relative_time(elapsed, period),
                "event_type": "gk_in" if action == "on" else "gk_out",
                "team": team_name,
                "player": player_name(gc.get("player")),
                "player_jersey": (gc.get("player") or {}).get("playerJerseyNr", ""),
                "secondary_player_1": "",
                "secondary_player_2": "",
                "score_state": "",
                "goal_type": "",
                "is_empty_net": "",
                "is_penalty_shot": "",
                "penalty_minutes": "",
                "penalty_reason": "",
                "on_ice_home": "",
                "on_ice_away": "",
            })

    # Sort by elapsed time, then assign event index
    events.sort(key=lambda e: e["elapsed_seconds"])
    for idx, ev in enumerate(events):
        ev["event_idx"] = idx

    return events


# ── Player stats extractor ────────────────────────────────────────────────────

def extract_player_rows(data: dict, game_id: int) -> list[dict[str, Any]]:
    """Extract per-game player stats for later aggregation."""
    gd = data.get("gameData", {})
    home_team = gd.get("homeTeamLongname", "HOME")
    away_team = gd.get("awayTeamLongname", "AWAY")
    rows: list[dict[str, Any]] = []

    for team_key, player_key, team_name in (
        ("homeFieldPlayers", "homeFieldPlayers", home_team),
        ("awayFieldPlayers", "awayFieldPlayers", away_team),
        ("homeGoalKeepers", "homeGoalKeepers", home_team),
        ("awayGoalKeepers", "awayGoalKeepers", away_team),
    ):
        for p in data.get(player_key, []):
            rows.append({
                "game_id": game_id,
                "player": player_name_stats(p),
                "team": team_name,
                "jersey": p.get("playerJerseyNr", ""),
                "position": p.get("position", ""),
                "goals": p.get("goals", 0) or 0,
                "assists": p.get("assists", 0) or 0,
                "pim": p.get("penaltyMinutes", 0) or 0,
                "plus_minus": p.get("plusMinus", 0) or 0,
                "shots": p.get("shotsOnGoal", 0) or 0,
                "toi": p.get("timeOnIce", 0) or 0,
            })

    return rows


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        print(f"  (empty — skipping {path})")
        return
    keys = list({k for row in rows for k in row})
    priority = [
        "game_id", "date", "home_team", "away_team", "score",
        "event_idx", "period", "time", "elapsed_seconds", "event_type",
        "team", "player", "player_jersey",
    ]
    cols = [c for c in priority if c in keys] + sorted(
        c for c in keys if c not in priority
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):,} rows → {path}")


# ── Player aggregation ────────────────────────────────────────────────────────

def build_players_csv(all_player_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not all_player_rows:
        return pd.DataFrame(
            columns=["player", "team", "goals", "assists", "points", "penalties", "pim"]
        )
    df = pd.DataFrame(all_player_rows)

    # Penalties = number of penalty events per player (from pim > 0 in game rows)
    df["penalties"] = (df["pim"] > 0).astype(int)

    agg = (
        df.groupby(["player", "team"], as_index=False)
        .agg(
            goals=("goals", "sum"),
            assists=("assists", "sum"),
            penalties=("penalties", "sum"),
            pim=("pim", "sum"),
        )
    )
    agg["points"] = agg["goals"] + agg["assists"]
    agg = agg[["player", "team", "goals", "assists", "points", "penalties", "pim"]]
    agg = agg.sort_values(
        ["points", "goals", "assists", "pim", "player"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    return agg


# ── Main scrape loop ──────────────────────────────────────────────────────────

def scrape_season(season: int = 2025) -> None:
    print(f"\n{'─'*60}")
    print(f"Scraping ICEHL season {season} …")

    print("  [1/3] Fetching schedule …")
    game_ids = get_game_ids(season)
    if not game_ids:
        print("  No games found — aborting.")
        return

    all_games: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    all_player_rows: list[dict[str, Any]] = []

    total = len(game_ids)
    print(f"  [2/3] Scraping {total} games …")
    for i, gid in enumerate(game_ids, 1):
        print(f"  [{i:3d}/{total}] Game {gid} … ", end="", flush=True)

        raw = fetch_json(MATCH_URL.format(game_id=gid))
        if raw is None or raw.get("statusId") != 1 or not raw.get("data"):
            print("no data — skipping")
            continue

        data = raw["data"]
        gd = data.get("gameData", {})
        home = gd.get("homeTeamShortname", "?")
        away = gd.get("awayTeamShortname", "?")

        game_row = parse_game_row(data, gid)
        all_games.append(game_row)

        events = parse_events(data, gid)
        all_events.extend(events)

        player_rows = extract_player_rows(data, gid)
        all_player_rows.extend(player_rows)

        print(
            f"OK  {home} {game_row['home_score']}:{game_row['away_score']} {away}"
            f"  ({len(events)} events, {len(player_rows)} player rows)"
        )
        time.sleep(DELAY)

    print(f"  [3/3] Writing CSVs …")
    write_csv(all_games, OUTPUT_DIR / "games.csv")
    write_csv(all_events, OUTPUT_DIR / "events.csv")

    players_df = build_players_csv(all_player_rows)
    players_path = OUTPUT_DIR / "players.csv"
    players_df.to_csv(players_path, index=False, encoding="utf-8-sig")
    print(f"  Saved {len(players_df):,} rows → {players_path}")

    print(f"\n{'─'*60}")
    print("Done!")
    print(f"  Games   : {len(all_games):,}")
    print(f"  Events  : {len(all_events):,}")
    print(f"  Players : {len(players_df):,}")


def scrape_single_game(game_id: int) -> None:
    print(f"Scraping game {game_id} …")
    raw = fetch_json(MATCH_URL.format(game_id=game_id))
    if raw is None or raw.get("statusId") != 1 or not raw.get("data"):
        print("  No data returned.")
        return

    data = raw["data"]
    game_row = parse_game_row(data, game_id)
    events = parse_events(data, game_id)
    player_rows = extract_player_rows(data, game_id)

    write_csv([game_row], OUTPUT_DIR / "games.csv")
    write_csv(events, OUTPUT_DIR / "events.csv")

    players_df = build_players_csv(player_rows)
    players_path = OUTPUT_DIR / "players.csv"
    players_df.to_csv(players_path, index=False, encoding="utf-8-sig")
    print(f"  Saved {len(players_df):,} rows → {players_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="ICEHL Austria stats scraper")
    parser.add_argument(
        "--season", type=int, default=2025,
        help="Season year (e.g. 2025 for 2025/26). Default: 2025",
    )
    parser.add_argument(
        "--game", type=int,
        help="Scrape a single game by ID (e.g. --game 7832)",
    )
    args = parser.parse_args()

    if args.game:
        scrape_single_game(args.game)
    else:
        scrape_season(args.season)


if __name__ == "__main__":
    main()
