#!/usr/bin/env python3
"""
SHL (Swedish Hockey League) — Full Season Stats Scraper
========================================================
Scrapes game data from stats.swehockey.se and outputs CSVs:
  - games.csv
  - events.csv
  - lineups.csv
  - reports.csv
  - players.csv

Main fixes versus the earlier version:
  - much stricter table detection
  - avoids parsing page-summary tables as events / lineups / reports
  - cleaner extraction of teams, score, date, spectators, venue
  - event parser only keeps rows that truly look like events
  - lineups / reports only keep rows that truly look like player rows
  - builds players.csv from goal + penalty events in the same style as the Slovak scraper
"""

from __future__ import annotations

import re
import sys
import csv
import time
import argparse
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests beautifulsoup4 pandas")
    sys.exit(1)

BASE = "https://stats.swehockey.se"
DELAY = 0.35
OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sv,en;q=0.9",
}


def fetch(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.content, "html.parser")
        except Exception as e:
            if attempt == retries - 1:
                print(f"    [!] Failed: {url} — {e}")
                return None
            time.sleep(1.5)
    return None


# ── Helpers ──────────────────────────────────────────────────────────────────

def clean(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def cell_texts(row) -> list[str]:
    return [clean(td.get_text(" ", strip=True)) for td in row.find_all(["td", "th"])]


def looks_like_time(text: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}:\d{2}", clean(text)))


def looks_like_score(text: str) -> bool:
    t = clean(text).replace("\xa0", " ")
    return bool(re.search(r"\b\d+\s*[-–]\s*\d+\b", t))


def normalize_name(name: Any) -> str:
    name = clean(name)
    if not name:
        return ""
    # remove repeated spaces and accidental glued jersey suffixes like Jhonas83
    name = re.sub(r"(?<=[A-Za-zÀ-ÖØ-öø-ÿ])\d+$", "", name).strip()
    return name


def first_match(pattern: str, text: str, flags: int = 0) -> str:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else ""


def split_teams(text: str) -> tuple[str, str]:
    # Normalize non-breaking spaces (\xa0) to regular spaces before splitting
    text = clean(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    for sep in [" - ", " – "]:
        if sep in text:
            left, right = text.split(sep, 1)
            return clean(left), clean(right)
    parts = re.split(r"\s[-–]\s", text, maxsplit=1)
    if len(parts) == 2:
        return clean(parts[0]), clean(parts[1])
    return "", ""


def looks_like_player_name(text: str) -> bool:
    t = clean(text)
    if not t:
        return False
    # common site format: Surname, Given
    if "," in t and re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", t):
        return True
    # fallback two words with letters
    if len(t.split()) >= 2 and re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", t):
        return True
    return False


def safe_int(text: Any) -> int | None:
    if text is None:
        return None
    s = re.sub(r"[^0-9-]", "", str(text))
    if s in {"", "-"}:
        return None
    try:
        return int(s)
    except ValueError:
        return None


# ── Schedule ─────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Clean and replace non-breaking spaces with regular spaces."""
    return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()


def _cell_text_plain(cell) -> str:
    """Get cell text excluding <i> elements (e.g. round-name labels in schedule)."""
    import copy
    c = copy.copy(cell)
    for tag in c.find_all("i"):
        tag.decompose()
    return _normalize(c.get_text())


def get_games_from_schedule(season_id: int) -> list[dict[str, Any]]:
    url = f"{BASE}/ScheduleAndResults/Schedule/{season_id}"
    soup = fetch(url)
    if soup is None:
        return []

    tbl = soup.find("table", class_="tblContent")
    if not tbl:
        return []

    rows = tbl.find_all("tr")
    if not rows:
        return []

    # rows[0] = title/section row (5 th)
    # rows[1] = column header row (7 th): "Round|Date|Game|..." or "Date|Time|Game|..."
    # rows[2+] = data rows (7 or 8 td)
    col_header_row = rows[1] if len(rows) > 1 else rows[0]
    col_header_texts = [_normalize(th.get_text()) for th in col_header_row.find_all(["td", "th"])]
    is_playoffs = bool(col_header_texts and col_header_texts[0].lower() == "round")

    games: list[dict[str, Any]] = []
    current_round: int | None = None

    for row in rows[2:]:
        cells = row.find_all("td")
        if not cells:
            continue
        texts = [_normalize(c.get_text()) for c in cells]

        if len(texts) < 4:
            continue

        # Extract game_id from javascript:openonlinewindow href or plain href
        game_id = None
        for a in row.find_all("a", href=True):
            m = re.search(r"/Game/Events/(\d+)", a["href"])
            if m:
                game_id = int(m.group(1))
                break

        if is_playoffs:
            # [0]=round_or_empty [1]=date+time [2]=teams [3]=score [4]=periods [5]=spec [6]=venue
            if texts[0].isdigit():
                current_round = int(texts[0])
            date_str    = texts[1]
            teams_str   = _cell_text_plain(cells[2])   # strip italic round label
            result_str  = texts[3]
            periods     = texts[4] if len(texts) > 4 else ""
            spectators  = texts[5] if len(texts) > 5 else ""
            venue       = texts[6] if len(texts) > 6 else ""
        else:
            # [0]=date (or time on continuation rows) [1]=dup datetime [2]=time [3]=teams
            # [4]=score [5]=periods [6]=spectators [7]=venue
            date_str   = texts[0] if re.match(r"\d{4}-\d{2}-\d{2}", texts[0]) else ""
            teams_str  = _cell_text_plain(cells[3])    # strip any italic labels
            result_str = texts[4] if len(texts) > 4 else ""
            periods    = texts[5] if len(texts) > 5 else ""
            spectators = texts[6] if len(texts) > 6 else ""
            venue      = texts[7] if len(texts) > 7 else ""

        home_team, away_team = split_teams(teams_str)
        if not home_team or not away_team:
            continue

        score = first_match(r"(\d+\s*[-–]\s*\d+)", result_str)

        games.append({
            "season_id":  season_id,
            "round":      current_round,
            "date":       date_str,
            "home_team":  home_team,
            "away_team":  away_team,
            "score":      score.replace(" ", "") if score else "",
            "periods":    periods,
            "spectators": spectators,
            "venue":      venue,
            "game_id":    game_id,
        })

    return games


# ── Events ───────────────────────────────────────────────────────────────────

def extract_meta_from_page(soup: BeautifulSoup, game_id: int, season_id: int) -> dict[str, Any]:
    meta: dict[str, Any] = {"game_id": game_id, "season_id": season_id}

    title = clean(soup.title.get_text(" ", strip=True) if soup.title else "")
    tm = re.match(r"(.+?)\s*-\s*(.+?)\s*\((\d+\s*[-–]\s*\d+)\)", title)
    if tm:
        meta["home_team_short"] = clean(tm.group(1))
        meta["away_team_short"] = clean(tm.group(2))
        meta["score"] = clean(tm.group(3)).replace(" ", "")

    whole = clean(soup.get_text(" ", strip=True))

    date_m = re.search(r"(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2})", whole)
    if date_m:
        meta["date"] = date_m.group(1)

    # Team names often appear around the title line before the competition label
    if "home_team" not in meta or "away_team" not in meta:
        m = re.search(r"([A-Za-zÀ-ÖØ-öø-ÿ .&'’-]+)\s*-\s*([A-Za-zÀ-ÖØ-öø-ÿ .&'’-]+)\s+20\d{2}-\d{2}-\d{2}", whole)
        if m:
            meta["home_team"] = clean(m.group(1))
            meta["away_team"] = clean(m.group(2))

    scoreline_m = re.search(r"(\d+)\s*[-–]\s*(\d+)\s*\(([^)]+)\)\s*Final Score", whole)
    if scoreline_m:
        meta["home_score"] = int(scoreline_m.group(1))
        meta["away_score"] = int(scoreline_m.group(2))
        meta["periods_detail"] = clean(scoreline_m.group(3))

    spec_m = re.search(r"Spectators:\s*([\d\s]+)", whole)
    if spec_m:
        meta["spectators"] = re.sub(r"\s+", "", spec_m.group(1))

    # Pick venue from lines near the date that are not competition labels
    text_lines = [clean(x) for x in soup.get_text("\n", strip=True).splitlines() if clean(x)]
    for i, line in enumerate(text_lines):
        if re.search(r"20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}", line):
            for cand in text_lines[i + 1 : i + 5]:
                if cand in {"Line Up", "Actions", "Reports"}:
                    continue
                if "SHL" in cand or "slutspel" in cand.lower() or "grundserie" in cand.lower():
                    continue
                if len(cand) >= 4:
                    meta.setdefault("venue", cand)
                    break
            break

    # summary metrics
    metric_patterns = {
        "home_shots": r"Shots\s*(\d+)\s*\([^)]*\)\s*\d+\s*[-–]\s*\d+",
        "away_shots": r"Final Score.*?Shots\s*(\d+)",
        "home_saves": r"Saves\s*(\d+)\s*\([^)]*\)\s*Saves",
        "away_saves": r"Saves\s*\d+\s*\([^)]*\)\s*Saves\s*(\d+)",
        "home_pim": r"PIM\s*(\d+)\s*\([^)]*\)\s*Line Up",
        "away_pim": r"PIM\s*(\d+)\s*\([^)]*\)\s*PP",
    }
    for key, pat in metric_patterns.items():
        m = re.search(pat, whole)
        if m:
            meta[key] = safe_int(m.group(1))

    pp_matches = re.findall(r"PP\s*([\d,.]+%)\s*\(([\d:]+)\)", whole)
    if len(pp_matches) >= 2:
        meta["home_pp_pct"], meta["home_pp_time"] = pp_matches[0]
        meta["away_pp_pct"], meta["away_pp_time"] = pp_matches[1]

    save_pct_matches = re.findall(r"(\d+[,.]\d+)%", whole)
    if len(save_pct_matches) >= 2:
        meta["home_save_pct"] = save_pct_matches[0]
        meta["away_save_pct"] = save_pct_matches[1]

    # If full team names still missing, fall back to schedule-style title lines.
    if not meta.get("home_team") or not meta.get("away_team"):
        for line in text_lines[:12]:
            if " - " in line and not re.search(r"\d{4}-\d{2}-\d{2}", line):
                left, right = split_teams(line)
                if left and right and len(left) > 2 and len(right) > 2:
                    meta.setdefault("home_team", left)
                    meta.setdefault("away_team", right)
                    break

    return meta


def detect_actions_table(soup: BeautifulSoup):
    """Find the innermost tblContent table that holds the game action events."""
    best_table = None
    best_score = 0
    for table in soup.find_all("table", class_="tblContent"):
        rows = table.find_all("tr")
        time_rows = sum(
            1 for r in rows
            if cell_texts(r) and looks_like_time(cell_texts(r)[0])
        )
        if time_rows > best_score:
            best_score = time_rows
            best_table = table
    return best_table if best_score >= 2 else None


def parse_event_row(texts: list[str], game_id: int, season_id: int, current_period: str | None) -> dict[str, Any] | None:
    # Event rows have exactly 5 columns: [time, type/score, team, player_info, extra]
    if len(texts) < 3 or not looks_like_time(texts[0]):
        return None

    time_str = texts[0]
    type_cell = clean(texts[1]) if len(texts) > 1 else ""
    team = clean(texts[2]) if len(texts) > 2 else ""
    player_cell = clean(texts[3]) if len(texts) > 3 else ""
    extra_cell = clean(texts[4]) if len(texts) > 4 else ""

    event_type = "unknown"
    score_state = None
    penalty_minutes = None
    penalty_reason = None
    penalty_start = None
    penalty_end = None

    # Goal: "1-0 (PP1)", "2-6 (EQ) ENG", "1-0 (SH)", etc.
    goal_m = re.match(r"(\d+-\d+)\s*\(([A-Z0-9]+)\)", type_cell)
    if goal_m:
        event_type = "goal"
        suffix = " ENG" if "ENG" in type_cell else ""
        score_state = f"{goal_m.group(1)} ({goal_m.group(2)}){suffix}"

    # GK events
    if "GK In" in type_cell:
        event_type = "gk_in"
    elif "GK Out" in type_cell:
        event_type = "gk_out"

    # Penalty: "2 min", "5 min", "10 min"
    pen_m = re.match(r"(\d+)\s*min", type_cell, flags=re.I)
    if pen_m and event_type == "unknown":
        event_type = "penalty"
        penalty_minutes = int(pen_m.group(1))
        rng = re.search(r"\((\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\)", extra_cell)
        if rng:
            penalty_start, penalty_end = rng.group(1), rng.group(2)
        reason_m = re.match(r"([^(]+)", extra_cell)
        if reason_m:
            penalty_reason = clean(reason_m.group(1)) or None

    # Extract players from player_cell — stop before "Pos. Part.:"
    player_section = re.split(r"\s*Pos\. Part\.:", player_cell)[0]
    # Format: "76. Levtchi, Anton 23. Sellgren, Jesper" — grab jersey + surname + first given-name word
    player_matches = re.findall(
        r"(\d+)\.\s*([A-Za-zÀ-ÖØ-öø-ÿ'][^,]+),\s*([A-Za-zÀ-ÖØ-öø-ÿ]\w*)",
        player_section,
    )

    # On-ice jersey numbers from extra_cell
    pos_m = re.search(r"Pos\. Part\.:\s*([\d ,]+?)(?=\s*Neg\. Part\.|$)", extra_cell)
    neg_m = re.search(r"Neg\. Part\.:\s*([\d ,]+?)$", extra_cell)

    event: dict[str, Any] = {
        "game_id": game_id,
        "season_id": season_id,
        "period": current_period,
        "time": time_str,
        "event_type": event_type,
        "team": team,
        "score_state": score_state,
        "penalty_minutes": penalty_minutes,
        "penalty_reason": penalty_reason,
        "penalty_start": penalty_start,
        "penalty_end": penalty_end,
        "on_ice_pos": clean(pos_m.group(1)) if pos_m else None,
        "on_ice_neg": clean(neg_m.group(1)) if neg_m else None,
    }

    for idx, (num, surname, given) in enumerate(player_matches[:3], 1):
        event[f"player{idx}_number"] = safe_int(num)
        event[f"player{idx}_name"] = normalize_name(f"{surname.strip()}, {given.strip()}")

    return event


def parse_events(game_id: int, season_id: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    url = f"{BASE}/Game/Events/{game_id}"
    soup = fetch(url)
    if soup is None:
        return {}, []

    meta = extract_meta_from_page(soup, game_id, season_id)
    events: list[dict[str, Any]] = []

    actions_table = detect_actions_table(soup)
    if not actions_table:
        return meta, events

    current_period = None
    for row in actions_table.find_all("tr"):
        texts = cell_texts(row)
        if not texts:
            continue

        joined = clean(" ".join(texts)).lower()
        if joined in {
            "1st period", "2nd period", "3rd period", "overtime", "game winning shots", "goalkeeper summary"
        } or any(p in joined for p in ["1st period", "2nd period", "3rd period", "overtime", "game winning shots", "goalkeeper summary"]):
            current_period = clean(" ".join(texts))
            continue

        event = parse_event_row(texts, game_id, season_id, current_period)
        if event:
            events.append(event)

    return meta, events


# ── Lineups ──────────────────────────────────────────────────────────────────

# Player entry format in cells: "55. Quenneville, David" or "46. Sjödin, Svante (LW)"
_PLAYER_RE = re.compile(
    r"(\d+)\.\s*"                             # jersey
    r"([A-Za-zÀ-ÖØ-öø-ÿ'][^,]+)"            # surname (up to comma)
    r",\s*"
    r"([A-Za-zÀ-ÖØ-öø-ÿ]\w*)"               # first word of given name
    r"(?:\s*\(([^)]+)\))?",                   # optional (POSITION)
    re.UNICODE,
)
_SECTION_KW = {"goalies", "1st line", "2nd line", "3rd line", "4th line", "5th line", "extra players"}
_TEAM_RE = re.compile(r"^([A-Za-zÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ\s&.\u2019'-]+?)\s*\(", re.UNICODE)
_SKIP_KW = ("line up", "referee", "linesmen", "head coach", "assistant coach", "last update")


def parse_lineups(game_id: int, season_id: int) -> list[dict[str, Any]]:
    url = f"{BASE}/Game/LineUps/{game_id}"
    soup = fetch(url)
    if soup is None:
        return []

    # Pick the tblContent table with the most player-looking cells
    best_table = None
    best_count = 0
    for t in soup.find_all("table", class_="tblContent"):
        count = sum(
            1 for r in t.find_all("tr")
            for c in cell_texts(r)
            if _PLAYER_RE.search(c) and len(c) < 200
        )
        if count > best_count:
            best_count = count
            best_table = t

    if not best_table:
        return []

    out: list[dict[str, Any]] = []
    current_team: str | None = None

    for row in best_table.find_all("tr"):
        cells = [clean(td.get_text(" ", strip=True)) for td in row.find_all(["td", "th"])]
        if not cells:
            continue

        # Skip huge aggregated/wrapper rows
        if any(len(c) > 200 for c in cells):
            continue

        non_empty = [c for c in cells if c]
        if not non_empty:
            continue

        first_lc = non_empty[0].lower()

        # Skip metadata rows
        if any(kw in first_lc for kw in _SKIP_KW):
            continue

        # Team header: single non-empty cell like "Örebro HK ()" or "Luleå HF ()"
        if len(non_empty) == 1 and first_lc not in _SECTION_KW and not _PLAYER_RE.search(non_empty[0]):
            m = _TEAM_RE.match(non_empty[0])
            if m:
                current_team = clean(m.group(1))
                continue

        # Extract all players from every cell in this row
        for cell in cells:
            if not cell or len(cell) > 200:
                continue
            if cell.lower() in _SECTION_KW:
                continue
            if any(kw in cell.lower() for kw in ("head coach", "assistant", "referee", "linesmen")):
                continue
            for pm in _PLAYER_RE.finditer(cell):
                num = safe_int(pm.group(1))
                surname = clean(pm.group(2))
                given = clean(pm.group(3))
                position = clean(pm.group(4)) if pm.group(4) else ""
                out.append({
                    "game_id": game_id,
                    "season_id": season_id,
                    "team": current_team,
                    "jersey": num,
                    "name": normalize_name(f"{surname}, {given}"),
                    "position": position,
                    "role": "skater",
                })

    return out


# ── Reports ──────────────────────────────────────────────────────────────────

def parse_reports(game_id: int, season_id: int) -> list[dict[str, Any]]:
    """The /Game/Reports/ page lists available official report documents and their timestamps."""
    url = f"{BASE}/Game/Reports/{game_id}"
    soup = fetch(url)
    if soup is None:
        return []

    reports: list[dict[str, Any]] = []
    for t in soup.find_all("table", class_="tblContent"):
        for row in t.find_all("tr"):
            texts = cell_texts(row)
            if len(texts) == 2 and texts[0] and texts[1]:
                if texts[0].lower() in {"report", "game reports"}:
                    continue
                if re.search(r"\d{4}-\d{2}-\d{2}", texts[1]):
                    reports.append({
                        "game_id": game_id,
                        "season_id": season_id,
                        "report_name": texts[0],
                        "created_at": texts[1],
                    })
    return reports


# ── Players aggregation ──────────────────────────────────────────────────────

def build_players_from_events(all_events: list[dict[str, Any]]) -> pd.DataFrame:
    events_df = pd.DataFrame(all_events)
    if events_df.empty:
        return pd.DataFrame(columns=["team", "player", "goals", "assists", "points", "penalties", "pim"])

    for c in ["team", "player1_name", "player2_name", "player3_name", "event_type", "penalty_minutes"]:
        if c not in events_df.columns:
            events_df[c] = None

    for c in ["team", "player1_name", "player2_name", "player3_name"]:
        events_df[c] = events_df[c].map(normalize_name)

    goal_events = events_df[events_df["event_type"] == "goal"].copy()
    pen_events = events_df[events_df["event_type"] == "penalty"].copy()

    goal_scorers = (
        goal_events.loc[goal_events["player1_name"].ne(""), ["team", "player1_name"]]
        .rename(columns={"player1_name": "player"})
        .assign(goals=1)
        .groupby(["team", "player"], as_index=False)["goals"]
        .sum()
    )

    assists1 = (
        goal_events.loc[goal_events["player2_name"].ne(""), ["team", "player2_name"]]
        .rename(columns={"player2_name": "player"})
        .assign(assists=1)
        .groupby(["team", "player"], as_index=False)["assists"]
        .sum()
    )

    assists2 = (
        goal_events.loc[goal_events["player3_name"].ne(""), ["team", "player3_name"]]
        .rename(columns={"player3_name": "player"})
        .assign(assists=1)
        .groupby(["team", "player"], as_index=False)["assists"]
        .sum()
    )

    penalties = (
        pen_events.loc[pen_events["player1_name"].ne(""), ["team", "player1_name", "penalty_minutes"]]
        .rename(columns={"player1_name": "player"})
        .assign(
            penalties=1,
            pim=lambda df: pd.to_numeric(df["penalty_minutes"], errors="coerce").fillna(0)
        )
        .groupby(["team", "player"], as_index=False)[["penalties", "pim"]]
        .sum()
    )

    players = goal_scorers.merge(assists1, on=["team", "player"], how="outer")
    players = players.merge(assists2, on=["team", "player"], how="outer", suffixes=("", "_2"))
    players = players.merge(penalties, on=["team", "player"], how="outer")

    for col in ["goals", "assists", "assists_2", "penalties", "pim"]:
        if col in players.columns:
            players[col] = players[col].fillna(0)

    if "assists_2" in players.columns:
        players["assists"] = players["assists"] + players["assists_2"]
        players = players.drop(columns=["assists_2"])

    for col in ["goals", "assists", "penalties", "pim"]:
        if col not in players.columns:
            players[col] = 0

    players["points"] = players["goals"] + players["assists"]
    players = players[["team", "player", "goals", "assists", "points", "penalties", "pim"]]
    players = players.sort_values(["points", "goals", "assists", "pim", "player"], ascending=[False, False, False, False, True]).reset_index(drop=True)
    return players


# ── CSV writer ───────────────────────────────────────────────────────────────

def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        print(f"  (empty — skipping {path})")
        return
    keys = list({k for row in rows for k in row})
    priority = [
        "game_id", "season_id", "date", "home_team", "away_team", "score",
        "round", "venue", "spectators", "period", "time", "event_type",
        "team", "jersey", "name", "position"
    ]
    cols = [c for c in priority if c in keys] + sorted(c for c in keys if c not in priority)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):,} rows → {path}")


# ── Main scrape loop ─────────────────────────────────────────────────────────

def scrape_season(season_id: int):
    print(f"\n{'─'*60}")
    print(f"Scraping season_id={season_id} …")

    print("  [1/4] Fetching schedule …")
    games = get_games_from_schedule(season_id)
    playable = [g for g in games if g.get("game_id")]
    print(f"        {len(games)} games found, {len(playable)} with detailed stats available.")

    all_games: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    all_lineups: list[dict[str, Any]] = []
    all_reports: list[dict[str, Any]] = []

    total = len(playable)
    for i, game in enumerate(playable, 1):
        gid = game["game_id"]
        print(f"  [{i:3d}/{total}] Game {gid}  {game['home_team']} vs {game['away_team']} … ", end="", flush=True)

        meta, events = parse_events(gid, season_id)
        merged = {**game, **meta}
        merged.setdefault("home_team", game.get("home_team"))
        merged.setdefault("away_team", game.get("away_team"))
        all_games.append(merged)
        all_events.extend(events)

        lineups = parse_lineups(gid, season_id)
        all_lineups.extend(lineups)

        reports = parse_reports(gid, season_id)
        all_reports.extend(reports)

        print(f"OK ({len(events)} events, {len(lineups)} lineup rows, {len(reports)} report rows)")
        time.sleep(DELAY)

    return all_games, all_events, all_lineups, all_reports


def main() -> None:
    parser = argparse.ArgumentParser(description="SHL stats scraper")
    parser.add_argument("--season-id", type=int, nargs="+", help="One or more season IDs (e.g. 19791 18507)")
    parser.add_argument("--find-season", type=str, help="Reserved for manual use; pass explicit season IDs when possible")
    args = parser.parse_args()

    # 18263 = SHL 2025-26 regular season, 19791 = SM-slutspel (playoffs)
    season_ids = args.season_id or [18263, 19791]
    if not args.season_id:
        print("No season specified — defaulting to 2025-26 regular season (18263) + playoffs (19791)")

    all_games: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    all_lineups: list[dict[str, Any]] = []
    all_reports: list[dict[str, Any]] = []

    for sid in season_ids:
        g, e, l, r = scrape_season(sid)
        all_games.extend(g)
        all_events.extend(e)
        all_lineups.extend(l)
        all_reports.extend(r)

    print(f"\n{'─'*60}")
    print("Writing CSVs …")
    write_csv(all_games, OUTPUT_DIR / "games.csv")
    write_csv(all_events, OUTPUT_DIR / "events.csv")
    write_csv(all_lineups, OUTPUT_DIR / "lineups.csv")
    write_csv(all_reports, OUTPUT_DIR / "reports.csv")

    players_df = build_players_from_events(all_events)
    players_path = OUTPUT_DIR / "players.csv"
    players_df.to_csv(players_path, index=False, encoding="utf-8-sig")
    print(f"  Saved {len(players_df):,} rows → {players_path}")

    print("\nDone!")
    print(f"  Games   : {len(all_games):,}")
    print(f"  Events  : {len(all_events):,}")
    print(f"  Lineups : {len(all_lineups):,}")
    print(f"  Reports : {len(all_reports):,}")
    print(f"  Players : {len(players_df):,}")


if __name__ == "__main__":
    main()
