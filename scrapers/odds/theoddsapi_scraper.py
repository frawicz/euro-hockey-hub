#!/usr/bin/env python3
"""
The Odds API scraper for European hockey leagues.

Fetches pre-game moneylines, puck-lines (spreads), and totals from
https://api.the-odds-api.com/v4 across all leagues in this project.

Environment:
    THE_ODDS_API_KEY=your_api_key          (required)

Usage:
    python scrapers/odds/theoddsapi_scraper.py
    python scrapers/odds/theoddsapi_scraper.py --leagues del liiga shl
    python scrapers/odds/theoddsapi_scraper.py --markets h2h totals
    python scrapers/odds/theoddsapi_scraper.py --list-sports
    python scrapers/odds/theoddsapi_scraper.py --scores-days 3   # enrich with recent results

Outputs (appended on each run, deduped by game_id + bookmaker + market + outcome_type):
    scrapers/odds/data/input/theoddsapi_games.csv

API quota:
    Each /odds call costs 1 request per bookmaker returned.
    Remaining quota is printed after every call (X-Requests-Remaining header).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("Missing dep. Run: pip install requests")
    sys.exit(1)

API_ROOT = "https://api.the-odds-api.com/v4"
DELAY = 0.5
OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_FILE = OUTPUT_DIR / "theoddsapi_games.csv"

# Map project league keys → The Odds API sport keys
# Run --list-sports to see what's currently live on the API
LEAGUE_MAP: dict[str, dict[str, str]] = {
    "germany":     {"sport_key": "icehockey_germany_del",         "label": "DEL"},
    "finland":     {"sport_key": "icehockey_finland_liiga",        "label": "Liiga"},
    "sweden":      {"sport_key": "icehockey_sweden_shl",           "label": "SHL"},
    "czech":       {"sport_key": "icehockey_czech_extraliga",      "label": "Czech Extraliga"},
    "austria":     {"sport_key": "icehockey_austria",              "label": "ICE Hockey League"},
    "slovakia":    {"sport_key": "icehockey_slovakia_extraliga",   "label": "Slovak Extraliga"},
    "switzerland": {"sport_key": "icehockey_switzerland_nl",       "label": "National League"},
    "khl":         {"sport_key": "icehockey_khl",                  "label": "KHL"},
}

OUTPUT_FIELDS = [
    "source", "league", "league_label", "game_id",
    "commence_time", "home_team", "away_team",
    "home_score", "away_score", "status",
    "bookmaker", "market",
    "outcome_type",   # "home" | "away" | "draw" | "over" | "under"
    "outcome_name",   # team name or "Over" / "Under"
    "european_odds",  # decimal odds
    "point",          # spread / total line (null for h2h)
    "last_update",
    "fetched_at",
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def get_api_key() -> str:
    key = os.environ.get("THE_ODDS_API_KEY", "").strip()
    if not key:
        print("Missing THE_ODDS_API_KEY environment variable.")
        print("Get a free key at https://the-odds-api.com/")
        print("Then: export THE_ODDS_API_KEY='your_key_here'")
        sys.exit(1)
    return key


def fetch(url: str, params: dict, retries: int = 3) -> tuple[Any, dict]:
    """Returns (json_body, response_headers). On failure returns (None, {})."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            remaining = resp.headers.get("x-requests-remaining", "?")
            used = resp.headers.get("x-requests-used", "?")
            resp.raise_for_status()
            return resp.json(), {"remaining": remaining, "used": used}
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 401:
                print("  [!] 401 Unauthorized — check your API key.")
                sys.exit(1)
            if exc.response is not None and exc.response.status_code == 422:
                # sport key not found / not active
                return None, {}
            if attempt == retries - 1:
                print(f"  [!] HTTP error: {exc}")
                return None, {}
        except Exception as exc:
            if attempt == retries - 1:
                print(f"  [!] Request failed: {exc}")
                return None, {}
        time.sleep(1.5)
    return None, {}


# ── Odds endpoints ────────────────────────────────────────────────────────────

def list_sports(api_key: str) -> list[dict]:
    data, _ = fetch(f"{API_ROOT}/sports", {"apiKey": api_key})
    return data if isinstance(data, list) else []


def get_odds(
    api_key: str,
    sport_key: str,
    markets: list[str],
    regions: list[str],
) -> tuple[list[dict], dict]:
    params = {
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": ",".join(markets),
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    data, quota = fetch(f"{API_ROOT}/sports/{sport_key}/odds", params)
    return (data if isinstance(data, list) else []), quota


def get_scores(api_key: str, sport_key: str, days_from: int = 3) -> list[dict]:
    params = {
        "apiKey": api_key,
        "daysFrom": days_from,
        "dateFormat": "iso",
    }
    data, _ = fetch(f"{API_ROOT}/sports/{sport_key}/scores", params)
    return data if isinstance(data, list) else []


# ── Flattening ────────────────────────────────────────────────────────────────

def _outcome_type(name: str, home_team: str, away_team: str, market: str) -> str:
    if market in ("spreads", "totals"):
        nl = name.lower()
        if nl == "over":
            return "over"
        if nl == "under":
            return "under"
        if name == home_team:
            return "home"
        if name == away_team:
            return "away"
        return "home" if name < away_team else "away"  # fallback alphabetic
    nl = name.lower()
    if nl == "draw":
        return "draw"
    if name == home_team:
        return "home"
    if name == away_team:
        return "away"
    return "unknown"


def flatten_odds(
    event: dict,
    league: str,
    label: str,
    scores_index: dict[str, dict],
    fetched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict] = []
    game_id = event.get("id", "")
    home_team = event.get("home_team", "")
    away_team = event.get("away_team", "")
    commence_time = event.get("commence_time", "")

    # Enrich with score data if available
    score_data = scores_index.get(game_id, {})
    home_score = score_data.get("home_score", "")
    away_score = score_data.get("away_score", "")
    status = "final" if score_data.get("completed") else "upcoming"

    for bookmaker in event.get("bookmakers", []):
        bk_key = bookmaker.get("key", "")
        bk_title = bookmaker.get("title", bk_key)
        last_update = bookmaker.get("last_update", "")

        for mkt in bookmaker.get("markets", []):
            mkt_key = mkt.get("key", "")

            for outcome in mkt.get("outcomes", []):
                name = outcome.get("name", "")
                rows.append({
                    "source": "theoddsapi",
                    "league": league,
                    "league_label": label,
                    "game_id": game_id,
                    "commence_time": commence_time,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "status": status,
                    "bookmaker": bk_title,
                    "market": mkt_key,
                    "outcome_type": _outcome_type(name, home_team, away_team, mkt_key),
                    "outcome_name": name,
                    "european_odds": outcome.get("price", ""),
                    "point": outcome.get("point", ""),
                    "last_update": last_update,
                    "fetched_at": fetched_at,
                })
    return rows


def build_scores_index(scores: list[dict]) -> dict[str, dict]:
    """game_id → {completed, home_score, away_score}"""
    idx: dict[str, dict] = {}
    for s in scores:
        gid = s.get("id", "")
        if not gid:
            continue
        home_team = s.get("home_team", "")
        away_team = s.get("away_team", "")
        home_score = ""
        away_score = ""
        for sc in s.get("scores") or []:
            if sc.get("name") == home_team:
                home_score = sc.get("score", "")
            elif sc.get("name") == away_team:
                away_score = sc.get("score", "")
        idx[gid] = {
            "completed": bool(s.get("completed")),
            "home_score": home_score,
            "away_score": away_score,
        }
    return idx


# ── CSV output ────────────────────────────────────────────────────────────────

def load_existing_keys(path: Path) -> set[tuple]:
    """Return set of (game_id, bookmaker, market, outcome_type) already on disk."""
    if not path.exists():
        return set()
    keys: set[tuple] = set()
    try:
        with path.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                keys.add((row.get("game_id", ""), row.get("bookmaker", ""),
                          row.get("market", ""), row.get("outcome_type", "")))
    except Exception:
        pass
    return keys


def append_csv(rows: list[dict], path: Path) -> int:
    """Append new rows to CSV; return count written."""
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)

    existing = load_existing_keys(path)
    new_rows = [
        r for r in rows
        if (r["game_id"], r["bookmaker"], r["market"], r["outcome_type"]) not in existing
    ]
    if not new_rows:
        return 0

    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)
    return len(new_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(
    leagues: list[str],
    markets: list[str],
    regions: list[str],
    scores_days: int,
    out: Path,
) -> None:
    api_key = get_api_key()
    fetched_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'─' * 60}")
    print(f"The Odds API ingest — {len(leagues)} league(s), markets: {markets}")
    print(f"Fetched at: {fetched_at}\n")

    total_written = 0

    for league in leagues:
        cfg = LEAGUE_MAP[league]
        sport_key = cfg["sport_key"]
        label = cfg["label"]

        print(f"  [{label}] sport_key={sport_key}")

        # Optionally fetch scores for result enrichment
        scores_index: dict[str, dict] = {}
        if scores_days > 0:
            scores = get_scores(api_key, sport_key, days_from=scores_days)
            scores_index = build_scores_index(scores)
            print(f"    Scores fetched: {len(scores_index)} game(s)")
            time.sleep(DELAY)

        # Fetch odds
        events, quota = get_odds(api_key, sport_key, markets, regions)
        if events is None:
            print(f"    (no data — sport key may be inactive this season)")
            time.sleep(DELAY)
            continue

        print(f"    Games with odds: {len(events)} | API quota remaining: {quota.get('remaining', '?')}")

        all_rows: list[dict] = []
        for event in events:
            all_rows.extend(flatten_odds(event, league, label, scores_index, fetched_at))

        written = append_csv(all_rows, out)
        total_written += written
        print(f"    Rows written: {written:,} (of {len(all_rows):,} fetched)")

        time.sleep(DELAY)

    print(f"\nDone. Total new rows: {total_written:,} → {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="The Odds API — European hockey odds ingestor")
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=list(LEAGUE_MAP.keys()),
        choices=list(LEAGUE_MAP.keys()),
        help="Leagues to fetch (default: all)",
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["h2h"],
        choices=["h2h", "spreads", "totals"],
        help="Betting markets (default: h2h only)",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        default=["eu", "uk"],
        choices=["eu", "uk", "us", "us2", "au"],
        help="Sportsbook regions (default: eu uk)",
    )
    parser.add_argument(
        "--scores-days",
        type=int,
        default=3,
        metavar="N",
        help="Enrich with scores from the last N days (0 to skip, default: 3)",
    )
    parser.add_argument(
        "--list-sports",
        action="store_true",
        help="Print all available hockey sports and exit",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: scrapers/odds/data/input/theoddsapi_games.csv)",
    )
    args = parser.parse_args()

    if args.list_sports:
        api_key = get_api_key()
        sports = list_sports(api_key)
        hockey = [s for s in sports if "hockey" in s.get("group", "").lower() or "hockey" in s.get("key", "").lower()]
        if not hockey:
            hockey = sports  # fall back to all if filter misses
        print(f"\n{'─' * 60}")
        print(f"{'Key':<45} {'Title':<30} {'Active'}")
        print("─" * 80)
        for s in sorted(hockey, key=lambda x: x.get("key", "")):
            print(f"{s.get('key',''):<45} {s.get('title',''):<30} {s.get('active', False)}")
        return

    out = Path(args.out) if args.out else OUTPUT_FILE
    run(
        leagues=args.leagues,
        markets=args.markets,
        regions=args.regions,
        scores_days=args.scores_days,
        out=out,
    )


if __name__ == "__main__":
    main()
