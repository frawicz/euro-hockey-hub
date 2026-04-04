#!/usr/bin/env python3
"""
SportsDataIO odds ingester.

This script uses SportsDataIO's Aggregated Odds API rather than scraping HTML.
It currently supports:
- betting events by season
- betting events by date
- betting markets by event
- optional sportsbook group filtering

Outputs:
  scrapers/odds/data/input/odds_events.csv
  scrapers/odds/data/input/odds_markets.csv
  scrapers/odds/data/input/odds_outcomes.csv

Environment:
  SPORTSDATAIO_KEY=your_api_key

Examples:
  python scrapers/odds/sportsdataio_odds.py --sport nhl --season 2026
  python scrapers/odds/sportsdataio_odds.py --sport nhl --date 2026-04-02
  python scrapers/odds/sportsdataio_odds.py --sport nhl --season 2026 --sportsbook-group G1001
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("Missing dep. Run: pip install requests")
    sys.exit(1)


API_ROOT = "https://api.sportsdata.io/v3"
DELAY = 0.35
OUTPUT_DIR = Path(__file__).parent / "data" / "input"


def get_api_key() -> str:
    key = os.environ.get("SPORTSDATAIO_KEY", "").strip()
    if not key:
        print("Missing SPORTSDATAIO_KEY environment variable.")
        print("Example: export SPORTSDATAIO_KEY='your_key_here'")
        sys.exit(1)
    return key


def build_headers(api_key: str) -> dict[str, str]:
    return {
        "Ocp-Apim-Subscription-Key": api_key,
        "Accept": "application/json",
        "User-Agent": "Euro-Hockey-Hub/1.0",
    }


def fetch_json(url: str, headers: dict[str, str], retries: int = 3) -> Any:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == retries - 1:
                print(f"    [!] Failed: {url} -- {exc}")
                return None
            time.sleep(1.5)
    return None


def odds_base(sport: str) -> str:
    return f"{API_ROOT}/{sport}/odds/json"


def get_events_by_season(sport: str, season: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    url = f"{odds_base(sport)}/BettingEventsBySeason/{season}"
    data = fetch_json(url, headers)
    return data if isinstance(data, list) else []


def get_events_by_date(sport: str, day: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    url = f"{odds_base(sport)}/BettingEventsByDate/{day}"
    data = fetch_json(url, headers)
    return data if isinstance(data, list) else []


def get_markets_by_event(
    sport: str,
    event_id: int | str,
    headers: dict[str, str],
    sportsbook_group: str | None = None,
) -> list[dict[str, Any]]:
    if sportsbook_group:
        url = f"{odds_base(sport)}/BettingMarketsByEvent/{event_id}/{sportsbook_group}"
    else:
        url = f"{odds_base(sport)}/BettingMarketsByEvent/{event_id}"
    data = fetch_json(url, headers)
    return data if isinstance(data, list) else []


def choose_first(d: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in d and d.get(key) is not None:
            return d.get(key)
    return default


def flatten_event(row: dict[str, Any], sport: str, season: str | None, sportsbook_group: str | None) -> dict[str, Any]:
    return {
        "sport": sport,
        "season": season or "",
        "sportsbook_group": sportsbook_group or "",
        "betting_event_id": choose_first(row, "BettingEventID", "BettingEventId"),
        "game_id": choose_first(row, "GameID", "GameId"),
        "global_game_id": choose_first(row, "GlobalGameID", "GlobalGameId"),
        "game_key": choose_first(row, "GameKey"),
        "season_type": choose_first(row, "SeasonType"),
        "status": choose_first(row, "Status"),
        "name": choose_first(row, "Name"),
        "team_id": choose_first(row, "TeamID", "TeamId"),
        "team_key": choose_first(row, "TeamKey"),
        "team": choose_first(row, "Team"),
        "home_team_id": choose_first(row, "HomeTeamID", "HomeTeamId"),
        "away_team_id": choose_first(row, "AwayTeamID", "AwayTeamId"),
        "home_team_key": choose_first(row, "HomeTeamKey"),
        "away_team_key": choose_first(row, "AwayTeamKey"),
        "home_team": choose_first(row, "HomeTeamName", "HomeTeam"),
        "away_team": choose_first(row, "AwayTeamName", "AwayTeam"),
        "start_date": choose_first(row, "StartDate"),
        "created": choose_first(row, "Created"),
        "updated": choose_first(row, "Updated", "LastUpdated"),
    }


def flatten_market(
    market: dict[str, Any],
    sport: str,
    season: str | None,
    sportsbook_group: str | None,
    event_row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sport": sport,
        "season": season or "",
        "sportsbook_group": sportsbook_group or choose_first(market, "SportsbookGroupID", "SportsbookGroupId"),
        "betting_event_id": choose_first(market, "BettingEventID", "BettingEventId", default=event_row.get("betting_event_id", "")),
        "betting_market_id": choose_first(market, "BettingMarketID", "BettingMarketId"),
        "game_id": choose_first(market, "GameID", "GameId", default=event_row.get("game_id", "")),
        "sportsbook_id": choose_first(market, "SportsbookID", "SportsbookId"),
        "sportsbook": choose_first(market, "Sportsbook"),
        "betting_market_type_id": choose_first(market, "BettingMarketTypeID", "BettingMarketTypeId"),
        "betting_market_type": choose_first(market, "BettingMarketType"),
        "betting_bet_type_id": choose_first(market, "BettingBetTypeID", "BettingBetTypeId"),
        "betting_bet_type": choose_first(market, "BettingBetType"),
        "betting_period_type_id": choose_first(market, "BettingPeriodTypeID", "BettingPeriodTypeId"),
        "betting_period_type": choose_first(market, "BettingPeriodType"),
        "name": choose_first(market, "Name"),
        "is_main": choose_first(market, "IsMain"),
        "is_off_the_board": choose_first(market, "IsOffTheBoard"),
        "is_suspended": choose_first(market, "IsSuspended"),
        "any_bets_available": choose_first(market, "AnyBetsAvailable"),
        "available": choose_first(market, "Available"),
        "created": choose_first(market, "Created"),
        "updated": choose_first(market, "Updated", "LastUpdated"),
    }


def flatten_outcome(
    outcome: dict[str, Any],
    sport: str,
    season: str | None,
    sportsbook_group: str | None,
    market_row: dict[str, Any],
    event_row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sport": sport,
        "season": season or "",
        "sportsbook_group": sportsbook_group or market_row.get("sportsbook_group", ""),
        "betting_event_id": market_row.get("betting_event_id", event_row.get("betting_event_id", "")),
        "betting_market_id": market_row.get("betting_market_id", ""),
        "betting_outcome_id": choose_first(outcome, "BettingOutcomeID", "BettingOutcomeId"),
        "game_id": market_row.get("game_id", event_row.get("game_id", "")),
        "sportsbook_id": market_row.get("sportsbook_id", ""),
        "sportsbook": market_row.get("sportsbook", ""),
        "betting_outcome_type_id": choose_first(outcome, "BettingOutcomeTypeID", "BettingOutcomeTypeId"),
        "betting_outcome_type": choose_first(outcome, "BettingOutcomeType"),
        "participant": choose_first(outcome, "Participant"),
        "player_id": choose_first(outcome, "PlayerID", "PlayerId"),
        "player_name": choose_first(outcome, "PlayerName"),
        "team_id": choose_first(outcome, "TeamID", "TeamId"),
        "team": choose_first(outcome, "Team"),
        "price_american": choose_first(outcome, "AmericanOdds", "Price"),
        "price_decimal": choose_first(outcome, "DecimalOdds"),
        "bet_value": choose_first(outcome, "BetValue"),
        "value": choose_first(outcome, "Value"),
        "payout": choose_first(outcome, "Payout"),
        "is_available": choose_first(outcome, "IsAvailable", "Available"),
        "is_suspended": choose_first(outcome, "IsSuspended"),
        "result_type": choose_first(outcome, "BettingResultType"),
        "created": choose_first(outcome, "Created"),
        "updated": choose_first(outcome, "Updated", "LastUpdated"),
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        print(f"  (empty -- skipping {path})")
        return

    keys = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows):>6,} rows -> {path}")


def scrape(
    sport: str,
    season: str | None,
    day: str | None,
    sportsbook_group: str | None,
    limit_events: int | None,
    output_dir: Path,
) -> None:
    api_key = get_api_key()
    headers = build_headers(api_key)

    if not season and not day:
        raise ValueError("Provide either --season or --date.")

    if season and day:
        raise ValueError("Use either --season or --date, not both.")

    print("\n" + "─" * 60)
    print(f"SportsDataIO odds ingest: sport={sport} season={season or '-'} date={day or '-'}")

    if season:
        events_raw = get_events_by_season(sport, season, headers)
    else:
        events_raw = get_events_by_date(sport, day, headers)

    if limit_events is not None:
        events_raw = events_raw[:limit_events]

    print(f"  Found {len(events_raw)} betting events")

    event_rows: list[dict[str, Any]] = []
    market_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []

    for idx, event in enumerate(events_raw, start=1):
        event_row = flatten_event(event, sport=sport, season=season, sportsbook_group=sportsbook_group)
        event_rows.append(event_row)
        event_id = event_row.get("betting_event_id")

        print(f"  [{idx:3d}/{len(events_raw):3d}] event={event_id} {event_row.get('away_team','')} @ {event_row.get('home_team','')}")
        if not event_id:
            continue

        markets = get_markets_by_event(sport, event_id, headers, sportsbook_group=sportsbook_group)
        for market in markets:
            market_row = flatten_market(market, sport=sport, season=season, sportsbook_group=sportsbook_group, event_row=event_row)
            market_rows.append(market_row)

            outcomes = choose_first(market, "BettingOutcomes", "Outcomes", default=[])
            if not isinstance(outcomes, list):
                outcomes = []
            for outcome in outcomes:
                outcome_rows.append(
                    flatten_outcome(
                        outcome,
                        sport=sport,
                        season=season,
                        sportsbook_group=sportsbook_group,
                        market_row=market_row,
                        event_row=event_row,
                    )
                )

        time.sleep(DELAY)

    write_csv(event_rows, output_dir / "odds_events.csv")
    write_csv(market_rows, output_dir / "odds_markets.csv")
    write_csv(outcome_rows, output_dir / "odds_outcomes.csv")

    print("\nDone.")
    print(f"  Events   : {len(event_rows):,}")
    print(f"  Markets  : {len(market_rows):,}")
    print(f"  Outcomes : {len(outcome_rows):,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="SportsDataIO odds ingester")
    parser.add_argument("--sport", default="nhl", help="SportsDataIO sport path, e.g. nhl, nfl, nba, mlb")
    parser.add_argument("--season", default=None, help="Season identifier used by SportsDataIO, e.g. 2026 for NHL")
    parser.add_argument("--date", default=None, help="Single date in YYYY-MM-DD format")
    parser.add_argument("--sportsbook-group", default=None, help="Optional Sportsbook Group ID, e.g. G1001")
    parser.add_argument("--limit-events", type=int, default=None, help="Only ingest the first N events for testing")
    parser.add_argument("--out", default=None, help="Output directory (default: scrapers/odds/data/input)")
    args = parser.parse_args()

    out = Path(args.out) if args.out else OUTPUT_DIR
    scrape(
        sport=args.sport,
        season=args.season,
        day=args.date,
        sportsbook_group=args.sportsbook_group,
        limit_events=args.limit_events,
        output_dir=out,
    )


if __name__ == "__main__":
    main()
