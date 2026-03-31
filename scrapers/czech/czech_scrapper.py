#!/usr/bin/env python3
"""
Czech Tipsport Extraliga — Scraper
====================================
Schedule:   https://json.esports.cz/hokejcz/scoreboard/YYYY-MM-DD.json
            League key "101" = Tipsport extraliga
Per-game:   https://www.hokej.cz/zapas/{hokejcz_id}/  (static HTML)

No Playwright required.

Output → scrapers/czech/data/input/:
  games.csv    — one row per game (metadata, score, period breakdown, team stats)
  events.csv   — goals and penalties (one row per event)
  players.csv  — season-aggregated skater stats
  goalies.csv  — season-aggregated goalie stats

Usage:
    python scrapers/czech/czech_scrapper.py                     # 2025-26 season
    python scrapers/czech/czech_scrapper.py --season 2024-25
    python scrapers/czech/czech_scrapper.py --game 2921796
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

try:
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests pandas beautifulsoup4")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SCOREBOARD_URL = "https://json.esports.cz/hokejcz/scoreboard/{date}.json"
GAME_URL       = "https://www.hokej.cz/zapas/{game_id}/"
LEAGUE_KEY     = "101"   # Tipsport extraliga in the scoreboard JSON
DELAY          = 0.4     # polite delay between per-game requests

OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "cs,en;q=0.9",
}

# Season date ranges (inclusive)
SEASON_DATES = {
    "2025-26": (date(2025, 9, 1),  date(2026, 4, 30)),
    "2024-25": (date(2024, 9, 1),  date(2025, 4, 30)),
    "2023-24": (date(2023, 9, 1),  date(2024, 4, 30)),
    "2022-23": (date(2022, 9, 1),  date(2023, 4, 30)),
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch_json(url: str) -> Any:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                print(f"    [!] JSON failed: {url} — {e}")
                return None
            time.sleep(1.5)
    return None


def fetch_html(url: str) -> BeautifulSoup | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            if attempt == 2:
                print(f"    [!] HTML failed: {url} — {e}")
                return None
            time.sleep(1.5)
    return None


# ── Schedule discovery ────────────────────────────────────────────────────────

def get_game_ids(season: str = "2025-26") -> list[tuple[str, str]]:
    """Return list of (game_id, date_str) for all finished Extraliga games."""
    start, end = SEASON_DATES.get(season, SEASON_DATES["2025-26"])
    today = date.today()
    end = min(end, today)

    games: list[tuple[str, str]] = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        data = fetch_json(SCOREBOARD_URL.format(date=date_str))
        if data and LEAGUE_KEY in data:
            for m in data[LEAGUE_KEY].get("matches", []):
                if m.get("match_status") == "po zápase":
                    games.append((str(m["hokejcz_id"]), date_str))
        current += timedelta(days=1)

    print(f"  Found {len(games)} finished games for {season}")
    return games


# ── HTML parsers ──────────────────────────────────────────────────────────────

def _cell_text(td) -> str:
    return td.get_text(separator=" ", strip=True)


def parse_game(soup: BeautifulSoup, game_id: str, date_str: str) -> dict[str, Any]:
    """Extract game metadata from the match-score section and summary table."""
    ms = soup.find(class_="match-score")
    home_team = away_team = home_abbr = away_abbr = ""
    home_score = away_score = ""
    period_scores = ot_flag = ""

    if ms:
        home_div = ms.find(class_="team-home")
        away_div = ms.find(class_="team-visiting")
        if home_div:
            home_team = (home_div.find("h2", class_="long") or home_div.find("h2") or home_div).get_text(strip=True)
            abbr_el = home_div.find("h2", class_="short")
            home_abbr = abbr_el.get_text(strip=True) if abbr_el else ""
        if away_div:
            away_team = (away_div.find("h2", class_="long") or away_div.find("h2") or away_div).get_text(strip=True)
            abbr_el = away_div.find("h2", class_="short")
            away_abbr = abbr_el.get_text(strip=True) if abbr_el else ""

        score_div = ms.find(class_="score")
        if score_div:
            home_score = (score_div.find(class_="home") or score_div).get_text(strip=True)
            away_score = (score_div.find(class_="visiting") or score_div).get_text(strip=True)
            spans = score_div.find_all("span")
            # Last span after "konec"/"end" is period scores
            for sp in spans:
                t = sp.get_text(strip=True)
                if re.match(r"\d+:\d+,", t):
                    period_scores = t

    # Check for OT/shootout from period scores count or suffix
    is_overtime = is_shootout = 0
    if period_scores:
        parts = [p.strip() for p in period_scores.split(",") if p.strip()]
        if len(parts) == 4:
            is_overtime = 1

    # Summary table (last table with class table-first-bold)
    summary = {}
    last_tbl = soup.find("table", class_="table-first-bold")
    if last_tbl:
        for row in last_tbl.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(separator=" ", strip=True)
                summary[key] = val

    def split_stat(key: str) -> tuple[str, str]:
        v = summary.get(key, "")
        parts = v.split(":")
        return (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("", "")

    shots_h, shots_a = split_stat("Střely na branku")
    pim_h,   pim_a   = split_stat("Trestné minuty")
    hits_h,  hits_a  = split_stat("Hity")
    fo_h,    fo_a    = split_stat("Vhazování")

    return {
        "game_id":        game_id,
        "date":           date_str,
        "home_team":      home_team,
        "away_team":      away_team,
        "home_abbr":      home_abbr,
        "away_abbr":      away_abbr,
        "home_score":     home_score,
        "away_score":     away_score,
        "score":          f"{home_score}:{away_score}",
        "period_scores":  period_scores,
        "is_overtime":    is_overtime,
        "is_shootout":    is_shootout,
        "home_shots":     shots_h,
        "away_shots":     shots_a,
        "home_pim":       pim_h,
        "away_pim":       pim_a,
        "home_hits":      hits_h,
        "away_hits":      hits_a,
        "home_faceoffs":  fo_h,
        "away_faceoffs":  fo_a,
    }


def parse_events(soup: BeautifulSoup, game_id: str, date_str: str) -> list[dict[str, Any]]:
    """Extract goals and penalties from per-period tables."""
    events: list[dict[str, Any]] = []
    event_idx = 0

    # Goals tables: class="table-last-right" with th.tbl-row-goals
    # Penalties tables: plain tables with th.tbl-row-penalty
    # Each set of 3 corresponds to periods 1, 2, 3 (and possibly OT)

    goals_tables   = [t for t in soup.find_all("table")
                      if t.find("th", class_="tbl-row-goals")]
    penalty_tables = [t for t in soup.find_all("table")
                      if t.find("th", class_="tbl-row-penalty")]

    def period_label(idx: int) -> int:
        return idx + 1  # 1-indexed; period 4 = OT

    for period_idx, tbl in enumerate(goals_tables):
        period = period_label(period_idx)
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            # Skip hidden plus/minus rows
            if "hide" in (row.get("class") or []):
                continue

            time_str = cells[0].get_text(strip=True)
            if not re.match(r"\d+:\d+", time_str):
                continue

            team = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            # Scorer cell
            scorer = ""
            scorer_goals = ""
            if len(cells) > 2:
                sc_cell = cells[2]
                a_tag = sc_cell.find("a")
                if a_tag:
                    full = a_tag.get_text(strip=True)
                    m = re.match(r"(.+?)\s*\((\d+)\)", full)
                    if m:
                        scorer = m.group(1).strip()
                        scorer_goals = m.group(2)
                    else:
                        scorer = full

            # Assists
            assist1 = assist2 = ""
            if len(cells) > 3:
                assists_cell = cells[3]
                a_tags = assists_cell.find_all("a")
                if len(a_tags) > 0:
                    a1_text = a_tags[0].get_text(strip=True)
                    ma = re.match(r"(.+?)\s*\((\d+)\)", a1_text)
                    assist1 = ma.group(1).strip() if ma else a1_text
                if len(a_tags) > 1:
                    a2_text = a_tags[1].get_text(strip=True)
                    ma = re.match(r"(.+?)\s*\((\d+)\)", a2_text)
                    assist2 = ma.group(1).strip() if ma else a2_text

            # Goal type (5/5, 5/4, 4/5, etc.)
            goal_type = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            events.append({
                "game_id":      game_id,
                "date":         date_str,
                "event_idx":    event_idx,
                "period":       period,
                "time":         time_str,
                "event_type":   "goal",
                "team":         team,
                "player":       scorer,
                "player_goals": scorer_goals,
                "assist1":      assist1,
                "assist2":      assist2,
                "goal_type":    goal_type,
                "penalty_min":  "",
                "penalty_reason": "",
            })
            event_idx += 1

    for period_idx, tbl in enumerate(penalty_tables):
        period = period_label(period_idx)
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            # Skip "no penalties" rows
            if len(cells) == 1 or (len(cells) >= 1 and not re.match(r"\d+:\d+", cells[0].get_text(strip=True))):
                continue

            time_str = cells[0].get_text(strip=True)
            if not re.match(r"\d+:\d+", time_str):
                continue

            team    = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            player  = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            penalty = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            # Extract minutes from e.g. "2 min. hákování"
            pen_m = re.match(r"(\d+)\s*min", penalty)
            pen_min = pen_m.group(1) if pen_m else ""
            pen_reason = re.sub(r"^\d+\s*min\.\s*", "", penalty).strip()

            events.append({
                "game_id":       game_id,
                "date":          date_str,
                "event_idx":     event_idx,
                "period":        period,
                "time":          time_str,
                "event_type":    "penalty",
                "team":          team,
                "player":        player,
                "player_goals":  "",
                "assist1":       "",
                "assist2":       "",
                "goal_type":     "",
                "penalty_min":   pen_min,
                "penalty_reason": pen_reason,
            })
            event_idx += 1

    events.sort(key=lambda e: (e["period"], e["time"]))
    for i, ev in enumerate(events):
        ev["event_idx"] = i

    return events


def parse_players(soup: BeautifulSoup, game_id: str) -> tuple[list[dict], list[dict]]:
    """Extract skater and goalie stats from the boxscore tables."""
    all_tables = soup.find_all("table")

    # Skater tables: have th with data-content="Čas na ledě" (TOI) and "Jméno hráče"
    # Goalie tables: have th with data-content="Úspěšnost [%]"
    skater_tables = []
    goalie_tables = []

    for t in all_tables:
        tips = [sp.get("data-content", "") for sp in t.find_all("span", class_="hasqtip")]
        if "Úspěšnost [%]" in tips:
            goalie_tables.append(t)
        elif "Čas na ledě" in tips and "Jméno hráče" in tips:
            skater_tables.append(t)

    ms = soup.find(class_="match-score")
    home_abbr = away_abbr = ""
    if ms:
        home_div = ms.find(class_="team-home")
        away_div = ms.find(class_="team-visiting")
        if home_div:
            el = home_div.find("h2", class_="short")
            home_abbr = el.get_text(strip=True) if el else ""
        if away_div:
            el = away_div.find("h2", class_="short")
            away_abbr = el.get_text(strip=True) if el else ""

    team_abbrs = [home_abbr, away_abbr]

    skaters: list[dict] = []
    for tbl_idx, tbl in enumerate(skater_tables):
        team = team_abbrs[tbl_idx] if tbl_idx < len(team_abbrs) else ""
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            jersey   = cells[0].get_text(strip=True)
            position = cells[1].get_text(strip=True)
            name_el  = cells[2].find("a")
            name     = name_el.get_text(strip=True) if name_el else cells[2].get_text(strip=True)

            def cell(i, default=""):
                if i >= len(cells):
                    return default
                v = cells[i].get("data-sort-value", "") or cells[i].get_text(strip=True)
                return v if v not in ("-100", "") else default

            skaters.append({
                "game_id":    game_id,
                "team":       team,
                "jersey":     jersey,
                "position":   position,
                "player":     name,
                "toi":        cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "pp_toi":     cells[4].get_text(strip=True) if len(cells) > 4 else "",
                "sh_toi":     cells[5].get_text(strip=True) if len(cells) > 5 else "",
                "goals":      _int(cell(6)),
                "assists":    _int(cell(7)),
                "points":     _int(cell(8)),
                "pim":        _int(cell(9)),
                "plus_minus": _int(cell(10)),
                "hits":       _int(cell(11)),
                "shots":      _int(cell(12)),
                "blocks":     _int(cell(13)),
            })

    goalies: list[dict] = []
    for tbl_idx, tbl in enumerate(goalie_tables):
        team = team_abbrs[tbl_idx] if tbl_idx < len(team_abbrs) else ""
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            jersey  = cells[0].get_text(strip=True)
            pos     = cells[1].get_text(strip=True)
            name_el = cells[2].find("a")
            name    = name_el.get_text(strip=True) if name_el else cells[2].get_text(strip=True)

            goalies.append({
                "game_id":   game_id,
                "team":      team,
                "jersey":    jersey,
                "position":  pos,
                "player":    name,
                "toi":       cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "saves":     _int(cells[4].get_text(strip=True) if len(cells) > 4 else ""),
                "goals_against": _int(cells[5].get_text(strip=True) if len(cells) > 5 else ""),
                "save_pct":  cells[6].get_text(strip=True) if len(cells) > 6 else "",
                "assists":   _int(cells[7].get_text(strip=True) if len(cells) > 7 else ""),
                "pim":       _int(cells[8].get_text(strip=True) if len(cells) > 8 else ""),
            })

    return skaters, goalies


def _int(v: str) -> int:
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return 0


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        print(f"  (empty — skipping {path.name})")
        return
    keys = list({k for row in rows for k in row})
    priority = [
        "game_id", "date", "home_team", "away_team", "score",
        "event_idx", "period", "time", "event_type",
        "team", "player",
    ]
    cols = [c for c in priority if c in keys] + sorted(c for c in keys if c not in priority)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):,} rows → {path}")


# ── Player/goalie aggregation ─────────────────────────────────────────────────

def aggregate_skaters(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    numeric = ["goals", "assists", "points", "pim", "plus_minus", "hits", "shots", "blocks"]
    for c in numeric:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    agg = (
        df.groupby(["player", "team", "position"], as_index=False)
        .agg(
            games=("game_id", "count"),
            goals=("goals", "sum"),
            assists=("assists", "sum"),
            pim=("pim", "sum"),
            plus_minus=("plus_minus", "sum"),
            hits=("hits", "sum"),
            shots=("shots", "sum"),
            blocks=("blocks", "sum"),
        )
    )
    agg["points"] = agg["goals"] + agg["assists"]
    agg = agg.sort_values(
        ["points", "goals", "assists", "player"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return agg[["player", "team", "position", "games", "goals", "assists", "points",
                "pim", "plus_minus", "hits", "shots", "blocks"]]


def aggregate_goalies(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["saves", "goals_against", "assists", "pim"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    # Only include rows where goalie actually played (toi not empty)
    df = df[df["toi"].str.strip() != ""]
    agg = (
        df.groupby(["player", "team"], as_index=False)
        .agg(
            games=("game_id", "count"),
            saves=("saves", "sum"),
            goals_against=("goals_against", "sum"),
            assists=("assists", "sum"),
            pim=("pim", "sum"),
        )
    )
    total_shots = agg["saves"] + agg["goals_against"]
    agg["save_pct"] = (agg["saves"] / total_shots.where(total_shots > 0, 1) * 100).round(2)
    agg["gaa"]      = (agg["goals_against"] / agg["games"].where(agg["games"] > 0, 1)).round(2)
    return agg.sort_values(["save_pct"], ascending=False).reset_index(drop=True)


# ── Main scrape loops ─────────────────────────────────────────────────────────

def scrape_season(season: str = "2025-26") -> None:
    print(f"\n{'─'*60}")
    print(f"Scraping Czech Tipsport Extraliga {season} …")

    print("  [1/3] Discovering finished games …")
    game_list = get_game_ids(season)
    if not game_list:
        print("  No finished games found — aborting.")
        return

    all_games: list[dict] = []
    all_events: list[dict] = []
    all_skater_rows: list[dict] = []
    all_goalie_rows: list[dict] = []

    total = len(game_list)
    print(f"  [2/3] Scraping {total} games …")
    for i, (gid, date_str) in enumerate(game_list, 1):
        print(f"  [{i:3d}/{total}] {gid} ({date_str}) … ", end="", flush=True)
        soup = fetch_html(GAME_URL.format(game_id=gid))
        if soup is None:
            print("failed")
            continue

        game_row = parse_game(soup, gid, date_str)
        all_games.append(game_row)

        events = parse_events(soup, gid, date_str)
        all_events.extend(events)

        skaters, goalies = parse_players(soup, gid)
        all_skater_rows.extend(skaters)
        all_goalie_rows.extend(goalies)

        home = game_row.get("home_abbr", "?")
        away = game_row.get("away_abbr", "?")
        hs   = game_row.get("home_score", "?")
        as_  = game_row.get("away_score", "?")
        goals = sum(1 for e in events if e["event_type"] == "goal")
        print(f"OK  {home} {hs}:{as_} {away}  ({goals} goals, {len(skaters)} skaters)")
        time.sleep(DELAY)

    print("  [3/3] Writing CSVs …")
    write_csv(all_games, OUTPUT_DIR / "games.csv")
    write_csv(all_events, OUTPUT_DIR / "events.csv")

    skaters_df = aggregate_skaters(all_skater_rows)
    skaters_df.to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(skaters_df):,} rows → {OUTPUT_DIR / 'players.csv'}")

    goalies_df = aggregate_goalies(all_goalie_rows)
    goalies_df.to_csv(OUTPUT_DIR / "goalies.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(goalies_df):,} rows → {OUTPUT_DIR / 'goalies.csv'}")

    print(f"\n{'─'*60}")
    print(f"Done!  Games: {len(all_games):,}  Events: {len(all_events):,}  "
          f"Players: {len(skaters_df):,}  Goalies: {len(goalies_df):,}")


def scrape_single_game(game_id: str) -> None:
    print(f"Scraping game {game_id} …")
    soup = fetch_html(GAME_URL.format(game_id=game_id))
    if soup is None:
        print("  No data returned.")
        return

    game_row = parse_game(soup, game_id, "")
    events   = parse_events(soup, game_id, "")
    skaters, goalies = parse_players(soup, game_id)

    write_csv([game_row], OUTPUT_DIR / "games.csv")
    write_csv(events,     OUTPUT_DIR / "events.csv")

    skaters_df = pd.DataFrame(skaters)
    skaters_df.to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(skaters_df):,} rows → {OUTPUT_DIR / 'players.csv'}")

    goalies_df = pd.DataFrame(goalies)
    goalies_df.to_csv(OUTPUT_DIR / "goalies.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(goalies_df):,} rows → {OUTPUT_DIR / 'goalies.csv'}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Czech Tipsport Extraliga scraper")
    parser.add_argument(
        "--season", default="2025-26",
        help="Season label, e.g. '2025-26' (default) or '2024-25'",
    )
    parser.add_argument(
        "--game", type=str,
        help="Scrape a single game by hokejcz ID, e.g. --game 2921796",
    )
    args = parser.parse_args()

    if args.game:
        scrape_single_game(args.game)
    else:
        scrape_season(args.season)


if __name__ == "__main__":
    main()
