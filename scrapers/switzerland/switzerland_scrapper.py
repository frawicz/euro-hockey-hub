#!/usr/bin/env python3
"""
Swiss National League (NL) — Season Stats Scraper
===================================================
Uses the undocumented but public National League Azure API:
  GET https://app-nationalleague-prod-001.azurewebsites.net/api/games
       → schedule for all seasons (filter by status / season param)
  GET https://app-nationalleague-prod-001.azurewebsites.net/api/games/{gameId}
       → full game detail (actions, player stats, lineups, team stats)

No Playwright / headless browser required.

Output → switzerland/data/input/:
  games.csv    — one row per game (metadata, score, period breakdown, team stats)
  events.csv   — goals, penalties, goalie changes (one row per event)
  players.csv  — season-aggregated stats per player

Usage:
    python switzerland/switzerland_scrapper.py               # 2025-26 season
    python switzerland/switzerland_scrapper.py --season 2024 # prior season
    python switzerland/switzerland_scrapper.py --game 20261105000001
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

API_BASE = "https://app-nationalleague-prod-001.azurewebsites.net"
GAMES_URL = f"{API_BASE}/api/games"
GAME_URL  = f"{API_BASE}/api/games/{{game_id}}"

OUTPUT_DIR = Path("switzerland/data/input")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DELAY = 0.35   # polite delay between per-game requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.nationalleague.ch",
    "Referer": "https://www.nationalleague.ch/",
}

# Season year suffix in gameId (e.g. 2026 → 2025-26 season)
# gameId format: YYYY1105NNNNNN where YYYY is the season end year
# e.g. 20261105000001 → 2025/26 season
SEASON_TO_YEAR = {
    "2025-26": "2026",
    "2024-25": "2025",
    "2023-24": "2024",
    "2022-23": "2023",
    "2021-22": "2022",
}

# ── HTTP helper ───────────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3) -> Any:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                print(f"    [!] Failed: {url} — {e}")
                return None
            time.sleep(1.5)
    return None


# ── Time helpers ──────────────────────────────────────────────────────────────

def elapsed_to_period_time(elapsed_sec: int) -> tuple[int, str]:
    """Convert total elapsed seconds to (period, MM:SS within period)."""
    # Periods 1-3 each 20 min (1200 s). OT is period 4.
    period = min((elapsed_sec // 1200) + 1, 4)
    start = (period - 1) * 1200
    rel = elapsed_sec - start
    return period, f"{rel // 60}:{rel % 60:02d}"


def action_period_time(a: dict) -> tuple[int, str]:
    """Return (period, time) for a game action from its 'time' and 'third' fields.

    The 'time' field is real wall-clock seconds from game start (not game-clock
    seconds), so we display it as total elapsed MM:SS without any period offset.
    """
    period = int(a.get("third", 1) or 1)
    try:
        elapsed = int(a.get("time", 0) or 0)
    except (ValueError, TypeError):
        elapsed = 0
    return period, f"{elapsed // 60}:{elapsed % 60:02d}"


# ── Schedule ──────────────────────────────────────────────────────────────────

def get_game_ids(season: str = "2025-26") -> list[str]:
    """Return all finished game IDs for the given season label."""
    year = SEASON_TO_YEAR.get(season, season.split("-")[1] if "-" in season else season)
    data = fetch_json(GAMES_URL)
    if data is None:
        return []

    # Filter by season year prefix in gameId and finished status
    ids = [
        g["gameId"]
        for g in data
        if str(g.get("gameId", "")).startswith(year)
        and g.get("status") == "finished"
    ]
    print(f"  Found {len(ids)} finished games for {season} (gameId prefix '{year}')")
    return sorted(ids)


# ── Game parser ───────────────────────────────────────────────────────────────

def parse_game_row(d: dict, game_id: str) -> dict[str, Any]:
    """Build games.csv row from the full game detail dict."""
    ov = d.get("overview", {})
    res = d.get("result", {})
    info = d.get("informations", {})
    hs = d.get("teamStatsHome", {})
    as_ = d.get("teamStatsAway", {})

    # Period scores from result
    def period_str(p: int) -> str:
        h = res.get(f"homeTeamFirstResult" if p == 1 else
                    f"homeTeamSecondResult" if p == 2 else
                    f"homeTeamThirdResult" if p == 3 else
                    f"homeTeamOvertimeResult", "")
        a = res.get(f"awayTeamFirstResult" if p == 1 else
                    f"awayTeamSecondResult" if p == 2 else
                    f"awayTeamThirdResult" if p == 3 else
                    f"awayTeamOvertimeResult", "")
        if h is None or a is None:
            return ""
        return f"{h}:{a}"

    periods = [period_str(i) for i in range(1, 4)]
    ot_score = ""
    if ov.get("isOvertime") or ov.get("isShootout"):
        ot_score = period_str(4)

    return {
        "game_id": game_id,
        "date": (ov.get("date", "") or "")[:10],
        "home_team": ov.get("homeTeamName", ""),
        "away_team": ov.get("awayTeamName", ""),
        "home_score": ov.get("homeTeamResult", ""),
        "away_score": ov.get("awayTeamResult", ""),
        "score": f"{ov.get('homeTeamResult','')}:{ov.get('awayTeamResult','')}",
        "period_scores": " | ".join(p for p in periods if p),
        "ot_score": ot_score,
        "is_overtime": int(bool(ov.get("isOvertime"))),
        "is_shootout": int(bool(ov.get("isShootout"))),
        "venue": ov.get("arena", "") or info.get("arena", ""),
        "spectators": ov.get("spectators", ""),
        "home_shots": hs.get("sog", ""),
        "away_shots": as_.get("sog", ""),
        "home_pim": hs.get("pim", ""),
        "away_pim": as_.get("pim", ""),
        "home_pp_goals": hs.get("ppg", ""),
        "home_pp_opps": hs.get("ppo", ""),
        "away_pp_goals": as_.get("ppg", ""),
        "away_pp_opps": as_.get("ppo", ""),
        "home_fow": hs.get("fow", ""),
        "away_fow": as_.get("fow", ""),
    }


# ── Events parser ─────────────────────────────────────────────────────────────

def parse_events(d: dict, game_id: str) -> list[dict[str, Any]]:
    """Extract goals, penalties, and goalie changes from game actions."""
    ov = d.get("overview", {})
    home_id = ov.get("homeTeamId", "")
    home_team = ov.get("homeTeamShortName", "HOME")
    away_team = ov.get("awayTeamShortName", "AWAY")

    events: list[dict[str, Any]] = []

    for period_block in d.get("actions", []):
        for a in period_block.get("actions", []):
            period, time_str = action_period_time(a)
            elapsed = int(a.get("time", 0) or 0)
            team = home_team if a.get("homeTeam") else away_team
            action_type = a.get("action", "")

            # Normalise action type
            if action_type == "goal":
                ev_type = "goal"
            elif action_type == "foul":
                ev_type = "penalty"
            elif action_type in ("in", "on"):
                ev_type = "gk_in"
            elif action_type in ("out", "off"):
                ev_type = "gk_out"
            else:
                ev_type = action_type

            score_state = ""
            if ev_type == "goal":
                h = a.get("homeTeamResult", "")
                aw = a.get("awayTeamResult", "")
                score_state = f"{h}:{aw}"

            events.append({
                "game_id": game_id,
                "elapsed_seconds": elapsed,
                "period": period,
                "time": time_str,
                "event_type": ev_type,
                "team": team,
                "player": _full_name(a.get("playerFirstName"), a.get("playerLastName")),
                "player_jersey": a.get("playerNumber", ""),
                "secondary_player_1": _full_name(a.get("assist1FirstName"), a.get("assist1LastName")),
                "secondary_player_2": _full_name(a.get("assist2FirstName"), a.get("assist2LastName")),
                "score_state": score_state,
                "goal_type": a.get("situation", "") or "",
                "is_empty_net": "",
                "penalty_minutes": a.get("foulMinutes", "") or "",
                "penalty_reason": "",   # API provides foulType (int), no text label
                "foul_type": a.get("foulType", "") or "",
            })

    events.sort(key=lambda e: e["elapsed_seconds"])
    for idx, ev in enumerate(events):
        ev["event_idx"] = idx

    return events


def _full_name(first: str | None, last: str | None) -> str:
    parts = [s.strip() for s in (first or "", last or "") if s and s.strip()]
    return " ".join(parts)


# ── Player stats extractor ────────────────────────────────────────────────────

def extract_player_rows(d: dict, game_id: str) -> list[dict[str, Any]]:
    ov = d.get("overview", {})
    home_team = ov.get("homeTeamName", "HOME")
    away_team = ov.get("awayTeamName", "AWAY")
    rows: list[dict[str, Any]] = []

    for team_key, team_name in (
        ("playerStatsHome", home_team),
        ("playerStatsAway", away_team),
    ):
        for p in d.get(team_key, []):
            rows.append({
                "game_id": game_id,
                "player": p.get("name", ""),
                "team": team_name,
                "jersey": p.get("number", ""),
                "position": p.get("position", ""),
                "goals": p.get("g", 0) or 0,
                "assists": (p.get("a1", 0) or 0) + (p.get("a2", 0) or 0),
                "pim": p.get("pim", 0) or 0,
                "plus_minus": p.get("plMi", 0) or 0,
                "shots": p.get("sog", 0) or 0,
                "toi": p.get("toi", 0) or 0,
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
    df["penalties"] = (df["pim"] > 0).astype(int)
    agg = (
        df.groupby(["player", "team"], as_index=False)
        .agg(goals=("goals", "sum"), assists=("assists", "sum"),
             penalties=("penalties", "sum"), pim=("pim", "sum"))
    )
    agg["points"] = agg["goals"] + agg["assists"]
    agg = agg[["player", "team", "goals", "assists", "points", "penalties", "pim"]]
    agg = agg.sort_values(
        ["points", "goals", "assists", "pim", "player"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    return agg


# ── Main scrape loops ─────────────────────────────────────────────────────────

def scrape_season(season: str = "2025-26") -> None:
    print(f"\n{'─'*60}")
    print(f"Scraping NL season {season} …")

    print("  [1/3] Fetching schedule …")
    game_ids = get_game_ids(season)
    if not game_ids:
        print("  No finished games found — aborting.")
        return

    all_games: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    all_player_rows: list[dict[str, Any]] = []

    total = len(game_ids)
    print(f"  [2/3] Scraping {total} games …")
    for i, gid in enumerate(game_ids, 1):
        print(f"  [{i:3d}/{total}] {gid} … ", end="", flush=True)
        raw = fetch_json(GAME_URL.format(game_id=gid))
        if raw is None:
            print("failed")
            continue

        ov = raw.get("overview", {})
        home = ov.get("homeTeamShortName", "?")
        away = ov.get("awayTeamShortName", "?")
        hs   = ov.get("homeTeamResult", "?")
        as_  = ov.get("awayTeamResult", "?")

        game_row = parse_game_row(raw, gid)
        all_games.append(game_row)

        events = parse_events(raw, gid)
        all_events.extend(events)

        player_rows = extract_player_rows(raw, gid)
        all_player_rows.extend(player_rows)

        print(f"OK  {home} {hs}:{as_} {away}  ({len(events)} events, {len(player_rows)} player rows)")
        time.sleep(DELAY)

    print(f"  [3/3] Writing CSVs …")
    write_csv(all_games, OUTPUT_DIR / "games.csv")
    write_csv(all_events, OUTPUT_DIR / "events.csv")

    players_df = build_players_csv(all_player_rows)
    players_df.to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(players_df):,} rows → {OUTPUT_DIR / 'players.csv'}")

    print(f"\n{'─'*60}")
    print(f"Done!  Games: {len(all_games):,}  Events: {len(all_events):,}  Players: {len(players_df):,}")


def scrape_single_game(game_id: str) -> None:
    print(f"Scraping game {game_id} …")
    raw = fetch_json(GAME_URL.format(game_id=game_id))
    if raw is None:
        print("  No data returned.")
        return

    game_row = parse_game_row(raw, game_id)
    events = parse_events(raw, game_id)
    player_rows = extract_player_rows(raw, game_id)

    write_csv([game_row], OUTPUT_DIR / "games.csv")
    write_csv(events, OUTPUT_DIR / "events.csv")

    players_df = build_players_csv(player_rows)
    players_df.to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(players_df):,} rows → {OUTPUT_DIR / 'players.csv'}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Swiss National League scraper")
    parser.add_argument(
        "--season", default="2025-26",
        help="Season label, e.g. '2025-26' (default) or '2024-25'",
    )
    parser.add_argument(
        "--game", type=str,
        help="Scrape a single game by ID, e.g. --game 20261105000001",
    )
    args = parser.parse_args()

    if args.game:
        scrape_single_game(args.game)
    else:
        scrape_season(args.season)


if __name__ == "__main__":
    main()
