#!/usr/bin/env python3
"""
KHL (Kontinental Hockey League) — Scraper
==========================================
Schedule:  Collect game IDs from each team's calendar page on khl.ru
           Stage 1369 = regular season 2025-26
           Stage 1370 = playoffs 2025-26
Per-game:  https://www.khl.ru/game/{stage}/{game_id}/protocol/  (static HTML)

No Playwright required — session cookies are obtained by hitting the homepage first.

Output → scrapers/khl/data/input/:
  games.csv    — one row per game (metadata, score, period breakdown, team stats)
  events.csv   — goals and penalties (one row per event)
  players.csv  — season-aggregated skater stats
  goalies.csv  — season-aggregated goalie stats

Usage:
    python scrapers/khl/khl_scrapper.py                     # 2025-26 regular season + playoffs
    python scrapers/khl/khl_scrapper.py --stage regular     # regular season only
    python scrapers/khl/khl_scrapper.py --stage playoffs    # playoffs only
    python scrapers/khl/khl_scrapper.py --game 1369/901687  # single game (stage/id)
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
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests pandas beautifulsoup4")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.khl.ru"
GAME_URL    = BASE_URL + "/game/{stage}/{game_id}/protocol/"

# KHL 2025-26 stage IDs
STAGE_REGULAR  = "1369"
STAGE_PLAYOFFS = "1370"

# All KHL teams with their site slugs (2025-26 season)
KHL_TEAMS = [
    "admiral", "ak_bars", "amur", "avangard", "avtomobilist", "barys",
    "cska", "dinamo_mn", "dragons", "dynamo_msk", "hc_sochi", "lada",
    "lokomotiv", "metallurg_mg", "neftekhimik", "salavat_yulaev",
    "severstal", "sibir", "ska", "spartak", "torpedo", "traktor",
]

OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DELAY = 0.4

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.9",
}


# ── Session ───────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    """Create a session with cookies from the homepage (required to avoid 403)."""
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(BASE_URL, timeout=15)
    except Exception:
        pass
    return s


_session: requests.Session | None = None

def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = make_session()
    return _session


def fetch_html(url: str) -> BeautifulSoup | None:
    s = get_session()
    for attempt in range(3):
        try:
            r = s.get(url, timeout=20)
            if r.status_code == 403:
                # Session expired — refresh
                global _session
                _session = make_session()
                r = _session.get(url, timeout=20)
            r.raise_for_status()
            if "<table" not in r.text and attempt < 2:
                time.sleep(1)
                continue
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            if attempt == 2:
                print(f"    [!] Failed: {url} — {e}")
                return None
            time.sleep(1.5)
    return None


# ── Schedule discovery ────────────────────────────────────────────────────────

def get_game_ids(stage: str) -> list[str]:
    """Collect all game IDs for a stage by scraping every team's calendar page."""
    all_ids: set[str] = set()
    for team in KHL_TEAMS:
        url = f"{BASE_URL}/clubs/{team}/calendar/"
        soup = fetch_html(url)
        if soup:
            ids = set(re.findall(rf"/game/{re.escape(stage)}/(\d+)/", str(soup)))
            all_ids.update(ids)
        time.sleep(0.2)
    return sorted(all_ids)


# ── HTML parsers ──────────────────────────────────────────────────────────────

def _cell_texts(row) -> list[str]:
    return [td.get_text(strip=True) for td in row.find_all(["td", "th"])]


def _txt(el) -> str:
    return el.get_text(strip=True) if el else ""


def parse_game(soup: BeautifulSoup, game_id: str, stage: str) -> dict[str, Any]:
    """Extract game metadata from the protocol page."""
    # Score section
    score_area = soup.find(class_=re.compile(r"match-score|game-score|score-area", re.I))
    home_team = away_team = home_abbr = away_abbr = ""
    home_score = away_score = period_scores = ""
    is_overtime = is_shootout = 0
    date_str = ""

    # Title format: "Team A - Team B: … матч КХЛ DD месяц YYYY"
    title = soup.title.string if soup.title else ""
    date_match = re.search(r"матч КХЛ (.+?)\s*[|\|]", title)
    if date_match:
        date_str = date_match.group(1).strip()

    # Extract teams from title "Team A - Team B:"
    tm = re.match(r"^(.+?)\s+-\s+(.+?):", title or "")
    if tm:
        home_team = tm.group(1).strip()
        away_team = tm.group(2).strip()

    # Score from slider-item__info-score (two p.slider-item__info-num elements)
    score_div = soup.find(class_="slider-item__info-score")
    if score_div:
        nums = score_div.find_all("p", class_="slider-item__info-num")
        if len(nums) >= 2:
            home_score = _txt(nums[0])
            away_score = _txt(nums[1])

    # Period scores: look for "X:Y, X:Y, X:Y" text near the score section
    page_text = soup.get_text()
    ps_match = re.search(r"(\d+:\d+(?:,\s*\d+:\d+){1,3})", page_text[:8000])
    if ps_match:
        period_scores = ps_match.group(1)

    # OT / shootout: goals table may have period "ОТ" or "БС"/"ПБ"/"РБ"
    tbls = soup.find_all("table")
    if tbls:
        goals_text = tbls[0].get_text()
        if "ОТ" in goals_text:
            is_overtime = 1
        if any(x in goals_text for x in ("БС", "ПБ", "РБ", "бул")):
            is_shootout = 1

    # Team stats from the last table (командные показатели)
    summary: dict[str, str] = {}
    tbls = soup.find_all("table")
    for t in reversed(tbls):
        rows = t.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = _txt(cells[0])
                val = _txt(cells[1])
                if key and val:
                    summary[key] = val

    return {
        "game_id":       game_id,
        "stage":         stage,
        "date":          date_str,
        "home_team":     home_team,
        "away_team":     away_team,
        "home_abbr":     home_abbr,
        "away_abbr":     away_abbr,
        "home_score":    home_score,
        "away_score":    away_score,
        "score":         f"{home_score}:{away_score}",
        "period_scores": period_scores,
        "is_overtime":   is_overtime,
        "is_shootout":   is_shootout,
    }


def parse_events(soup: BeautifulSoup, game_id: str, stage: str) -> list[dict[str, Any]]:
    """Extract goals (from table[0]) and penalties (from fineTable divs)."""
    events: list[dict[str, Any]] = []

    # ── Goals ──────────────────────────────────────────────────────────────────
    tables = soup.find_all("table")
    if tables:
        goals_table = tables[0]
        for row in goals_table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            texts = [_txt(c) for c in cells]
            # Goal rows: goal_num | period | time | score | situation | scorer | assist1 | assist2 | ...
            if not re.match(r"^\d+$", texts[0]):
                continue
            period_str = texts[1]
            # Time uses Unicode prime chars: 39′43′′ → 39:43
            time_raw = texts[2]
            time_str = re.sub(r"[′'′''ʹ]+", ":", time_raw)
            time_str = re.sub(r":+$", "", time_str)  # strip trailing colons
            score_state = texts[3]
            situation   = texts[4]   # рав. бол. мен. бул.
            # Scorer
            scorer_cell = cells[5] if len(cells) > 5 else None
            scorer = ""
            if scorer_cell:
                a = scorer_cell.find("a")
                scorer = _txt(a) if a else _txt(scorer_cell)
                scorer = re.sub(r"\s*\(\d+\)\s*$", "", scorer).strip()
            # Assists
            assist1 = assist2 = ""
            if len(cells) > 6:
                a_tags = cells[6].find_all("a")
                if a_tags:
                    assist1 = re.sub(r"\s*\(\d+\)\s*$", "", _txt(a_tags[0])).strip()
            if len(cells) > 7:
                a_tags = cells[7].find_all("a")
                if a_tags:
                    assist2 = re.sub(r"\s*\(\d+\)\s*$", "", _txt(a_tags[0])).strip()
            # Team: determined from on-ice columns — home team abbr in col 8, away in col 9
            # Use situation and score_state to infer; team abbr from header row
            team = ""  # We'll leave blank — hard to determine without more context

            events.append({
                "game_id":    game_id,
                "stage":      stage,
                "event_type": "goal",
                "period":     period_str,
                "time":       time_str,
                "team":       team,
                "player":     scorer,
                "assist1":    assist1,
                "assist2":    assist2,
                "goal_type":  situation,
                "score_state": score_state,
                "penalty_min": "",
                "penalty_reason": "",
            })

    # ── Penalties ──────────────────────────────────────────────────────────────
    fine_div = soup.find(class_="fineTable-item")
    if fine_div:
        # Two sides: left (home) and right (away)
        sides = fine_div.find_all(class_="fineTable-table")
        # Map side index to team
        home_abbr = away_abbr = ""
        header_clubs = fine_div.find_all(class_="fineTable-header__club-name")
        home_name = _txt(header_clubs[0]) if len(header_clubs) > 0 else "HOME"
        away_name = _txt(header_clubs[1]) if len(header_clubs) > 1 else "AWAY"

        for side_idx, side in enumerate(sides):
            team_name = home_name if side_idx == 0 else away_name
            for line in side.find_all(class_="fineTable-table__line-body"):
                items = line.find_all(class_="fineTable-table__line-item")
                if len(items) < 2:
                    continue
                # Item 0: time + player, Item 1: minutes + reason
                item0_texts = [_txt(p) for p in items[0].find_all("p") if _txt(p)]
                item1_texts = [_txt(p) for p in items[1].find_all("p") if _txt(p)]
                if not item0_texts or not item1_texts:
                    continue
                time_str = item0_texts[0] if item0_texts else ""
                if not re.match(r"\d+:\d+", time_str):
                    continue
                player_el = items[0].find("a")
                player = _txt(player_el) if player_el else (item0_texts[1] if len(item0_texts) > 1 else "")
                pen_min = item1_texts[0] if item1_texts else ""
                pen_reason = item1_texts[1] if len(item1_texts) > 1 else ""

                events.append({
                    "game_id":    game_id,
                    "stage":      stage,
                    "event_type": "penalty",
                    "period":     "",
                    "time":       time_str,
                    "team":       team_name,
                    "player":     player,
                    "assist1":    "",
                    "assist2":    "",
                    "goal_type":  "",
                    "score_state": "",
                    "penalty_min": pen_min,
                    "penalty_reason": pen_reason,
                })

    # Assign sequential index
    for i, ev in enumerate(events):
        ev["event_idx"] = i

    return events


def parse_players(soup: BeautifulSoup, game_id: str) -> tuple[list[dict], list[dict]]:
    """Extract skater and goalie stats from boxscore tables."""
    tables = soup.find_all("table")

    # Tables layout (0-indexed from the protocol page):
    # 0: goals events
    # 1: home goalies (cols: №, Игрок, И, В, П, ИБ, Бр, ПШ, ОБ, %ОБ, КН, Ш, А, И"0", Штр, ВП)
    # 2: home defensemen (cols: №, Игрок, И, Ш, А, О, +/-, +, -, Штр, ШР, ШБ, ШМ, ШО, ШП, РБ, БВ, %БВ, Вбр...)
    # 3: home forwards (same columns)
    # 4: away goalies
    # 5: away defensemen
    # 6: away forwards
    # 7+: team stats

    # Get team names from goals table header or title
    title = soup.title.string if soup.title else ""
    tm = re.match(r"^(.+?)\s+-\s+(.+?):", title)
    home_team = tm.group(1).strip() if tm else "HOME"
    away_team = tm.group(2).strip() if tm else "AWAY"

    skaters: list[dict] = []
    goalies: list[dict] = []

    # Identify goalie vs skater tables by column headers
    # Goalie: has "КН" (GAA) column title
    # Skater: has "+/-" column title
    def header_texts(t) -> list[str]:
        hrow = t.find("tr")
        return [th.get_text(strip=True) for th in hrow.find_all(["th", "td"])] if hrow else []

    def is_goalie_table(t) -> bool:
        # Goalie table has "ПШ" (goals against) and "КН" (GAA) but NOT "+/-"
        h = header_texts(t)
        return "ПШ" in h and "КН" in h and "+/-" not in h

    def is_skater_table(t) -> bool:
        h = header_texts(t)
        return "+/-" in h and "О" in h

    goalie_tables  = [t for t in tables if is_goalie_table(t)]
    skater_tables  = [t for t in tables if is_skater_table(t)]

    teams = [home_team, away_team]

    for t_idx, (tbl, team) in enumerate(zip(goalie_tables[:2], teams)):
        for row in tbl.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            jersey = _txt(cells[0])
            if not re.match(r"^\d+$", jersey):
                continue
            name_a = cells[1].find("a")
            name = _txt(name_a) if name_a else _txt(cells[1])
            if not name:
                continue
            # Goalie cols: №, Игрок(pos?), Игрок, И, В, П, ИБ, Бр, ПШ, ОБ, %ОБ, КН, Ш, А, И"0", Штр, ВП
            # Cols: №(0) Игрок(1) И(2) В(3) П(4) ИБ(5) Бр(6) ПШ(7) ОБ(8) %ОБ(9) КН(10) Ш(11) А(12) И"0"(13) Штр(14) ВП(15)
            def gcell(i):
                if i >= len(cells): return ""
                v = cells[i].get("data-sort-value", "") or _txt(cells[i])
                return "" if v in ("-100","") else v
            toi = gcell(15)  # ВП = time on ice
            if not toi:
                continue  # didn't play
            goalies.append({
                "game_id":       game_id,
                "team":          team,
                "jersey":        jersey,
                "player":        name,
                "toi":           toi,
                "shots_faced":   gcell(6),   # Бр
                "goals_against": gcell(7),   # ПШ
                "saves":         gcell(8),   # ОБ
                "save_pct":      gcell(9),   # %ОБ
                "gaa":           gcell(10),  # КН
                "shutouts":      gcell(13),  # И"0"
                "pim":           gcell(14),  # Штр
            })

    # Pair skater tables: [home_def, home_fwd, away_def, away_fwd]
    for t_idx, tbl in enumerate(skater_tables[:4]):
        team = teams[t_idx // 2]
        pos_label = "D" if t_idx % 2 == 0 else "F"
        for row in tbl.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            jersey = _txt(cells[0])
            if not re.match(r"^\d+$", jersey):
                continue
            name_a = cells[1].find("a")
            name = _txt(name_a) if name_a else _txt(cells[1])
            if not name:
                continue
            def scell(i):
                if i >= len(cells): return 0
                v = cells[i].get("data-sort-value", "") or _txt(cells[i])
                try: return int(float(v))
                except: return 0
            # Skater columns: №(0) Игрок(1) И(2) Ш(3) А(4) О(5) +/-(6) +(7) -(8)
            #   Штр(9) ШР(10) ШБ(11) ШМ(12) ШО(13) ШП(14) РБ(15) БВ(16) %БВ(17)
            #   Вбр(18) ... ВП(21) ... БлБ(33)
            skaters.append({
                "game_id":    game_id,
                "team":       team,
                "jersey":     jersey,
                "position":   pos_label,
                "player":     name,
                "toi":        _txt(cells[21]) if len(cells) > 21 else "",
                "goals":      scell(3),
                "assists":    scell(4),
                "points":     scell(5),
                "plus_minus": scell(6),
                "pim":        scell(9),
                "shots":      scell(11),
                "blocks":     scell(33) if len(cells) > 33 else 0,
            })

    return skaters, goalies


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        print(f"  (empty — skipping {path.name})")
        return
    keys = list({k for row in rows for k in row})
    priority = ["game_id", "stage", "date", "home_team", "away_team", "score",
                "event_idx", "period", "time", "event_type", "team", "player"]
    cols = [c for c in priority if c in keys] + sorted(c for c in keys if c not in priority)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):,} rows → {path}")


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate_skaters(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["goals", "assists", "points", "pim", "plus_minus", "shots", "blocks"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    agg = (df.groupby(["player", "team", "position"], as_index=False)
             .agg(games=("game_id","count"), goals=("goals","sum"),
                  assists=("assists","sum"), pim=("pim","sum"),
                  plus_minus=("plus_minus","sum"), shots=("shots","sum"),
                  blocks=("blocks","sum")))
    agg["points"] = agg["goals"] + agg["assists"]
    return agg.sort_values(["points","goals","player"], ascending=[False,False,True]).reset_index(drop=True)


def aggregate_goalies(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["shots_faced", "goals_against", "saves", "pim"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    agg = (df.groupby(["player", "team"], as_index=False)
             .agg(games=("game_id","count"), shots_faced=("shots_faced","sum"),
                  goals_against=("goals_against","sum"), saves=("saves","sum"),
                  pim=("pim","sum")))
    total = agg["saves"] + agg["goals_against"]
    agg["save_pct"] = (agg["saves"] / total.where(total > 0, 1) * 100).round(2)
    agg["gaa"]      = (agg["goals_against"] / agg["games"].where(agg["games"] > 0, 1)).round(2)
    return agg.sort_values("save_pct", ascending=False).reset_index(drop=True)


# ── Main loops ────────────────────────────────────────────────────────────────

def scrape_stage(stage: str, label: str) -> tuple[list, list, list, list]:
    print(f"  Discovering {label} game IDs (stage {stage}) …")
    game_ids = get_game_ids(stage)
    if not game_ids:
        print(f"  No games found for stage {stage}.")
        return [], [], [], []
    print(f"  Found {len(game_ids)} games. Scraping …")

    all_games, all_events, all_skaters, all_goalies = [], [], [], []
    total = len(game_ids)
    for i, gid in enumerate(game_ids, 1):
        print(f"  [{i:3d}/{total}] {stage}/{gid} … ", end="", flush=True)
        soup = fetch_html(GAME_URL.format(stage=stage, game_id=gid))
        if soup is None:
            print("failed")
            continue

        game_row = parse_game(soup, gid, stage)
        events   = parse_events(soup, gid, stage)
        skaters, goalies = parse_players(soup, gid)

        all_games.append(game_row)
        all_events.extend(events)
        all_skaters.extend(skaters)
        all_goalies.extend(goalies)

        hs = game_row.get("home_score", "?")
        as_ = game_row.get("away_score", "?")
        goals = sum(1 for e in events if e["event_type"] == "goal")
        pens  = sum(1 for e in events if e["event_type"] == "penalty")
        print(f"OK  {hs}:{as_}  ({goals}G {pens}P  {len(skaters)} sk)")
        time.sleep(DELAY)

    return all_games, all_events, all_skaters, all_goalies


def scrape_season(stages: list[str]) -> None:
    print(f"\n{'─'*60}")
    print("Scraping KHL 2025-26 …")

    all_games, all_events, all_skaters, all_goalies = [], [], [], []
    stage_labels = {STAGE_REGULAR: "regular season", STAGE_PLAYOFFS: "playoffs"}

    for stage in stages:
        g, e, sk, go = scrape_stage(stage, stage_labels.get(stage, f"stage {stage}"))
        all_games.extend(g)
        all_events.extend(e)
        all_skaters.extend(sk)
        all_goalies.extend(go)

    print(f"\n  Writing CSVs …")
    write_csv(all_games,  OUTPUT_DIR / "games.csv")
    write_csv(all_events, OUTPUT_DIR / "events.csv")

    skaters_df = aggregate_skaters(all_skaters)
    skaters_df.to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(skaters_df):,} rows → {OUTPUT_DIR / 'players.csv'}")

    goalies_df = aggregate_goalies(all_goalies)
    goalies_df.to_csv(OUTPUT_DIR / "goalies.csv", index=False, encoding="utf-8-sig")
    print(f"  Saved {len(goalies_df):,} rows → {OUTPUT_DIR / 'goalies.csv'}")

    print(f"\n{'─'*60}")
    print(f"Done!  Games: {len(all_games):,}  Events: {len(all_events):,}  "
          f"Players: {len(skaters_df):,}  Goalies: {len(goalies_df):,}")


def scrape_single(game_ref: str) -> None:
    """game_ref = 'stage/game_id' e.g. '1369/897491'"""
    if "/" in game_ref:
        stage, gid = game_ref.split("/", 1)
    else:
        stage, gid = STAGE_REGULAR, game_ref
    print(f"Scraping game {stage}/{gid} …")
    soup = fetch_html(GAME_URL.format(stage=stage, game_id=gid))
    if soup is None:
        print("  No data returned.")
        return
    game_row = parse_game(soup, gid, stage)
    events   = parse_events(soup, gid, stage)
    skaters, goalies = parse_players(soup, gid)

    write_csv([game_row], OUTPUT_DIR / "games.csv")
    write_csv(events,     OUTPUT_DIR / "events.csv")
    pd.DataFrame(skaters).to_csv(OUTPUT_DIR / "players.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(goalies).to_csv(OUTPUT_DIR / "goalies.csv", index=False, encoding="utf-8-sig")
    print(f"  {len(skaters)} skaters, {len(goalies)} goalies, {len(events)} events")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="KHL scraper")
    parser.add_argument("--stage", choices=["regular", "playoffs", "both"], default="both",
                        help="Which stage to scrape (default: both)")
    parser.add_argument("--game", type=str,
                        help="Scrape a single game: stage/game_id, e.g. --game 1369/897491")
    args = parser.parse_args()

    if args.game:
        scrape_single(args.game)
    elif args.stage == "regular":
        scrape_season([STAGE_REGULAR])
    elif args.stage == "playoffs":
        scrape_season([STAGE_PLAYOFFS])
    else:
        scrape_season([STAGE_REGULAR, STAGE_PLAYOFFS])


if __name__ == "__main__":
    main()
