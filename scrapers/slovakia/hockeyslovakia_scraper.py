"""
hockeyslovakia.sk scraper
Scrapes match data from the Tipsport Liga (Slovak Extraliga)

Usage:
    python hockeyslovakia_scraper.py                  # diagnose default match
    python hockeyslovakia_scraper.py --season         # scrape full season
    python hockeyslovakia_scraper.py --match 153021   # specific match ID
    python hockeyslovakia_scraper.py --players        # scrape rich official player stats
    python hockeyslovakia_scraper.py --players --with-gamelogs
    python hockeyslovakia_scraper.py --diagnose       # inspect page structure

Output: CSV / JSON files saved to ./data/
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────────

BASE_URL = "https://www.hockeyslovakia.sk"
SEASON_ID = 1131  # 2025-26 Tipsport Liga
LEAGUE_SLUG = "tipsport-liga"
OUTPUT_DIR = str(Path(__file__).parent / "data" / "input")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.hockeyslovakia.sk/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

os.makedirs(OUTPUT_DIR, exist_ok=True)

PLAYER_LIST_COLUMN_MAP = {
    "Meno": "player",
    "Rok": "birth_year",
    "Tím": "team_code",
    "Post": "position",
    "Z": "games",
    "G": "goals",
    "A": "assists",
    "B": "points",
    "+ / -": "plus_minus",
    "PIM": "pim",
    "P2": "penalties_2",
    "P5": "penalties_5",
    "P10": "penalties_10",
    "P20": "penalties_20",
    "P25": "penalties_25",
    "TOI": "toi",
}

PLAYER_PROFILE_TOTAL_MAP = {
    "Z": "games",
    "G": "goals",
    "A": "assists",
    "B": "points",
    "PTS_AVG": "points_per_game",
    "ESP": "even_strength_points",
    "PPP": "power_play_points",
    "SHP": "shorthanded_points",
    "+/-": "plus_minus",
    "PIM": "pim",
    "S": "shots",
    "GWG": "game_winning_goals",
    "GTG": "game_tying_goals",
    "FOT": "faceoffs_taken",
    "FOWP": "faceoff_win_pct",
    "H+": "hits",
    "TOI": "toi",
}

PLAYER_GAMELOG_MAP = {
    "Súper": "opponent_raw",
    "Dátum": "date",
    "Výsledok": "result",
    "G": "goals",
    "A": "assists",
    "B": "points",
    "+/-": "plus_minus",
    "PIM": "pim",
    "S": "shots",
    "GWG": "game_winning_goals",
    "GTG": "game_tying_goals",
    "FOT": "faceoffs_taken",
    "FOWP": "faceoff_win_pct",
    "H+": "hits",
    "TOI": "toi",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_soup(url: str) -> BeautifulSoup | None:
    """Fetch a page and return BeautifulSoup, or None on failure."""
    try:
        response = SESSION.get(url, timeout=20)
        if response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")
        print(f"  HTTP {response.status_code} for {url}")
        return None
    except requests.RequestException as exc:
        print(f"  Request error for {url}: {exc}")
        return None


def safe_text(el: Any) -> str:
    """Return stripped text from a BeautifulSoup element."""
    return el.get_text(" ", strip=True) if el else ""


def clean_whitespace(text: str) -> str:
    """Collapse repeated whitespace."""
    return re.sub(r"\s+", " ", text).strip()


def parse_numeric(value: Any) -> int | float | None:
    """Parse integers / decimal-comma numbers / percentages from strings."""
    if value is None:
        return None

    text = clean_whitespace(str(value))
    if not text or text in {"-", "—", "None", "nan"}:
        return None

    text = text.replace("%", "").replace(",", ".")
    text = text.replace("\xa0", "")
    text = text.rstrip(".")
    text = text.strip()

    if re.fullmatch(r"-?\d+", text):
        return int(text)

    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)

    return None


def parse_birth_date(value: str) -> str:
    """Convert DD.MM.YYYY strings to ISO YYYY-MM-DD when possible."""
    value = clean_whitespace(value)
    if not value:
        return ""

    ts = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(ts):
        return value
    return ts.strftime("%Y-%m-%d")


def parse_time_to_seconds(value: str) -> int | None:
    """Convert MM:SS-style time-on-ice strings to seconds."""
    value = clean_whitespace(value)
    match = re.fullmatch(r"(\d+):(\d{2})", value)
    if not match:
        return None

    minutes = int(match.group(1))
    seconds = int(match.group(2))
    return minutes * 60 + seconds


def extract_id_from_href(href: str, kind: str) -> int | None:
    """Extract numeric player/team IDs from profile URLs."""
    match = re.search(rf"/{kind}/(\d+)/", href or "")
    return int(match.group(1)) if match else None


def max_page_from_soup(soup: BeautifulSoup) -> int:
    """Find the largest pagination page number present on the page."""
    pages = [1]

    for link in soup.select("ul.pagination a[href], .pagination a[href]"):
        href = link.get("href", "")
        match = re.search(r"[?&]page=(\d+)", href)
        if match:
            pages.append(int(match.group(1)))

    return max(pages)


def extract_teams_from_title(title_text: str) -> tuple[str, str]:
    if not title_text:
        return "", ""

    parts = [p.strip() for p in title_text.split("|")]
    versus_part = next((p for p in parts if " vs. " in p), "")

    if not versus_part:
        return "", ""

    home, away = versus_part.split(" vs. ", 1)
    return clean_whitespace(home), clean_whitespace(away)


def parse_score(score_text: str) -> tuple[int | None, int | None]:
    """Parse final score like '2:4' into integers."""
    if not score_text:
        return None, None

    nums = re.findall(r"\d+", score_text)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])

    return None, None


def extract_event_tables(soup: BeautifulSoup) -> list[str]:
    """
    Collect text from one-row event tables.

    The site appears to use many separate mini-tables for events like:
    - goals
    - penalties
    - goalie in/out
    """
    texts: list[str] = []

    for table in soup.select("table"):
        rows = table.select("tr")
        if len(rows) != 1:
            continue

        text = clean_whitespace(safe_text(table))
        if not text:
            continue

        # Keep only likely event rows with a time marker
        if re.search(r"\b\d{1,2}:\d{2}\b", text):
            texts.append(text)

    return texts


def classify_event(event_text: str) -> str:
    """
    Roughly classify the event row.
    Returns one of: goal, penalty, goalie_change, other
    """
    lowered = event_text.lower()

    if "min." in lowered:
        return "penalty"

    if "in" in lowered or "out" in lowered:
        # Some goalie substitution rows look like:
        # 00:00 IN RIEČICKÝ Dominik
        # 56:45 OUT KIVIAHO Henri
        if re.search(r"\b(?:in|out)\b", lowered):
            return "goalie_change"

    # Goals often contain a changed score state like 0:1, 1:2, 2:4
    score_patterns = re.findall(r"\b\d+:\d+\b", event_text)
    time_patterns = re.findall(r"\b\d{1,2}:\d{2}\b", event_text)

    # If there is more than one score-like/time-like pattern, it may include a score event
    # Example: "19:14 19:14 0:1 PH1 PETRÁŠ Šimon ..."
    if len(score_patterns) >= 2 or (len(score_patterns) == 1 and len(time_patterns) >= 1):
        if "min." not in lowered and not re.search(r"\b(?:in|out)\b", lowered):
            return "goal"

    return "other"


def parse_penalty_event(event_text: str) -> dict[str, Any]:
    """
    Parse a penalty event approximately.

    Example:
    00:42 2 min. PETRISKA, Ján (Nedovolené bránenie v hre) 00:42
    """
    time_match = re.search(r"\b(\d{1,2}:\d{2})\b", event_text)
    mins_match = re.search(r"(\d+)\s*min\.", event_text, flags=re.IGNORECASE)
    player_match = re.search(
        r"\d+\s*min\.\s*([^(]+?)(?:\(|$)",
        event_text,
        flags=re.IGNORECASE,
    )
    reason_match = re.search(r"\(([^)]+)\)", event_text)

    return {
        "time": time_match.group(1) if time_match else "",
        "mins": mins_match.group(1) if mins_match else "",
        "player": clean_whitespace(player_match.group(1)) if player_match else "",
        "reason": clean_whitespace(reason_match.group(1)) if reason_match else "",
        "raw": event_text,
    }


def parse_goal_event(event_text: str) -> dict[str, Any]:
    """
    Parse a goal event approximately.

    Example fragments:
    19:14 19:14 0:1 PH1 PETRÁŠ Šimon (1) ROSANDIČ Mislav (1)
    47:59 47:59 2:4 LAMPER Patrik (1) JELLÚŠ Simon (1), BARTO...
    """
    time_match = re.search(r"\b(\d{1,2}:\d{2})\b", event_text)

    score_states = re.findall(r"\b\d+:\d+\b", event_text)
    score_state = score_states[-1] if score_states else ""

    goal_type_match = re.search(r"\b(PH1|PH2|SH|PP|EN|TS)\b", event_text)
    goal_type = goal_type_match.group(1) if goal_type_match else ""

    scorer = ""
    assists: list[str] = []

    # Remove leading time(s), score state, and simple type marker to isolate names
    cleaned = event_text
    cleaned = re.sub(r"^\s*\d{1,2}:\d{2}\s*", "", cleaned)
    cleaned = re.sub(r"^\s*\d{1,2}:\d{2}\s*", "", cleaned)
    cleaned = re.sub(r"\b\d+:\d+\b", "", cleaned, count=1)
    cleaned = re.sub(r"\b(PH1|PH2|SH|PP|EN|TS)\b", "", cleaned, count=1)
    cleaned = clean_whitespace(cleaned)

    # Split on assist separators if present
    # This is heuristic because the site formatting is messy
    parts = re.split(r"\s{2,}|,", cleaned)
    parts = [clean_whitespace(p) for p in parts if clean_whitespace(p)]

    if parts:
        scorer = parts[0]
        if len(parts) > 1:
            assists = parts[1:]

    return {
        "time": time_match.group(1) if time_match else "",
        "score_state": score_state,
        "type": goal_type,
        "scorer": scorer,
        "assists": assists,
        "raw": event_text,
    }


def parse_goalie_change_event(event_text: str) -> dict[str, Any]:
    """Parse goalie IN/OUT events approximately."""
    time_match = re.search(r"\b(\d{1,2}:\d{2})\b", event_text)
    action_match = re.search(r"\b(IN|OUT)\b", event_text, flags=re.IGNORECASE)
    player = re.sub(r"^\s*\d{1,2}:\d{2}\s*", "", event_text)
    player = re.sub(r"\b(IN|OUT)\b", "", player, flags=re.IGNORECASE)
    player = clean_whitespace(player)

    return {
        "time": time_match.group(1) if time_match else "",
        "action": action_match.group(1).upper() if action_match else "",
        "player": player,
        "raw": event_text,
    }


def extract_events(soup: BeautifulSoup) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """
    Extract goals, penalties, goalie changes, and unclassified events.
    """
    goals: list[dict[str, Any]] = []
    penalties: list[dict[str, Any]] = []
    goalie_changes: list[dict[str, Any]] = []
    others: list[str] = []

    for event_text in extract_event_tables(soup):
        event_type = classify_event(event_text)

        if event_type == "goal":
            goals.append(parse_goal_event(event_text))
        elif event_type == "penalty":
            penalties.append(parse_penalty_event(event_text))
        elif event_type == "goalie_change":
            goalie_changes.append(parse_goalie_change_event(event_text))
        else:
            others.append(event_text)

    return goals, penalties, goalie_changes, others


# ── Match list scraper ────────────────────────────────────────────────────────

def get_match_ids(
    season_id: int = SEASON_ID,
    center_match_id: int = 153021,
    window: int = 400,
    delay: float = 0.2,
) -> list[int]:
    """
    Discover match IDs by probing a numeric range around a known valid match ID.

    This avoids relying on a broken season listing page or weak internal links.
    """
    print(
        f"Scanning for valid match pages around ID {center_match_id} "
        f"(range {center_match_id - window} to {center_match_id + window})"
    )

    found_ids = []

    for match_id in range(center_match_id - window, center_match_id + window + 1):
        url = (
            f"{BASE_URL}/sk/stats/matches/{season_id}/{LEAGUE_SLUG}"
            f"/match/{match_id}/Overview"
        )

        try:
            response = SESSION.get(url, timeout=10)
        except requests.RequestException:
            continue

        if response.status_code != 200:
            time.sleep(delay)
            continue

        text = response.text

        # Skip pages that technically return 200 but are not real match pages
        if "HockeySlovakia.sk" not in text:
            time.sleep(delay)
            continue

        # Require a score block or title with "vs."
        has_score = 'class="score"' in text or "scoreperiod" in text
        has_vs = " vs. " in text

        if has_score or has_vs:
            found_ids.append(match_id)
            print(f"  Found match {match_id}")

        time.sleep(delay)

    print(f"  Found {len(found_ids)} valid match IDs")
    return found_ids


# ── Single match scraper ──────────────────────────────────────────────────────

def scrape_match(match_id: int, season_id: int = SEASON_ID) -> dict[str, Any]:
    """
    Scrape a single match overview page.
    Returns a dict with basic metadata and parsed event information.
    """
    url = (
        f"{BASE_URL}/sk/stats/matches/{season_id}/{LEAGUE_SLUG}"
        f"/match/{match_id}/Overview"
    )
    print(f"  Scraping match {match_id} ...")

    soup = get_soup(url)
    if not soup:
        return {"match_id": match_id, "error": "fetch_failed", "url": url}

    result: dict[str, Any] = {
        "match_id": match_id,
        "url": url,
    }

    # ── Title and teams ────────────────────────────────────────────────────
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    home_team, away_team = extract_teams_from_title(title_text)

    # ── Meta ───────────────────────────────────────────────────────────────
    meta: dict[str, Any] = {}

    date_el = soup.select_one(".match-date, .game-date, time")
    round_el = soup.select_one(".match-round, .round-number")
    venue_el = soup.select_one(".match-venue, .arena-name, .venue")
    attendance_el = soup.select_one(".match-attendance, .attendance")

    meta["date"] = safe_text(date_el)
    meta["round"] = safe_text(round_el)
    meta["venue"] = safe_text(venue_el)
    meta["attendance"] = safe_text(attendance_el)

    info_lines = []
    for row in soup.select(".match-info-row, .game-info tr, .match-header-info li"):
        text = clean_whitespace(safe_text(row))
        if text:
            info_lines.append(text)
    meta["info"] = info_lines

    result["meta"] = meta

    # ── Score ──────────────────────────────────────────────────────────────
    score_el = soup.select_one("span.score, .match-score, .score-result, .final-score")
    period_el = soup.select_one("span.scoreperiod, .scoreperiod, .period-score, .periods")

    score_text = clean_whitespace(safe_text(score_el))
    period_text = clean_whitespace(safe_text(period_el))

    home_goals, away_goals = parse_score(score_text)

    result["home_team"] = home_team
    result["away_team"] = away_team
    result["score"] = score_text
    result["home_goals"] = home_goals
    result["away_goals"] = away_goals
    result["period_scores"] = re.findall(r"\d+:\d+", period_text)

    # ── Events ─────────────────────────────────────────────────────────────
    goals, penalties, goalie_changes, other_events = extract_events(soup)

    result["goals"] = goals
    result["penalties"] = penalties
    result["goalie_changes"] = goalie_changes
    result["other_events"] = other_events

    # ── Still tentative / not reliably found on this page layout ──────────
    result["shots"] = {}
    result["players"] = []

    return result


# ── Flattening ────────────────────────────────────────────────────────────────

def flatten_match(data: dict[str, Any]) -> dict[str, Any]:
    """Flatten a match dict into a single-row dict for CSV export."""
    return {
        "match_id": data.get("match_id"),
        "date": data.get("meta", {}).get("date", ""),
        "round": data.get("meta", {}).get("round", ""),
        "venue": data.get("meta", {}).get("venue", ""),
        "attendance": data.get("meta", {}).get("attendance", ""),
        "home_team": data.get("home_team", ""),
        "away_team": data.get("away_team", ""),
        "score": data.get("score", ""),
        "home_goals": data.get("home_goals", ""),
        "away_goals": data.get("away_goals", ""),
        "periods": " | ".join(data.get("period_scores", [])),
        "goal_count": len(data.get("goals", [])),
        "penalty_count": len(data.get("penalties", [])),
        "goalie_change_count": len(data.get("goalie_changes", [])),
        "unclassified_event_count": len(data.get("other_events", [])),
        "url": data.get("url", ""),
        "error": data.get("error", ""),
    }


# ── Season scraper ────────────────────────────────────────────────────────────

def scrape_season(
    season_id: int = SEASON_ID,
    delay: float = 2.0,
    limit: int | None = None
) -> pd.DataFrame:
    match_ids = get_match_ids(season_id)

    if not match_ids:
        raise RuntimeError(
            "No match IDs were found. Try increasing the scan window."
        )

    if limit is not None:
        match_ids = match_ids[:limit]

    all_rows = []
    for i, mid in enumerate(match_ids):
        data = scrape_match(mid, season_id)
        all_rows.append(flatten_match(data))

        if i < len(match_ids) - 1:
            time.sleep(delay)

    df = pd.DataFrame(all_rows)
    path = f"{OUTPUT_DIR}/season_{season_id}_matches.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(df)} matches → {path}")
    return df


# ── Stats pages ───────────────────────────────────────────────────────────────

def extract_table_rows(table: Any) -> tuple[list[str], list[list[str]]]:
    """Return normalized headers and data rows from a stats table."""
    headers = [clean_whitespace(safe_text(th)) for th in table.select("th")]
    rows: list[list[str]] = []

    for tr in table.select("tr"):
        cells = [clean_whitespace(safe_text(td)) for td in tr.select("td")]
        if cells:
            rows.append(cells)

    return headers, rows


def scrape_generic_stats_table(url: str, output_path: str) -> pd.DataFrame:
    """Generic helper for simple leaderboard pages."""
    print(f"Fetching stats from: {url}")
    soup = get_soup(url)
    if not soup:
        return pd.DataFrame()

    table = soup.select_one("table.table, table.stats-table, table#players-stats, .players-stats table")
    if table is None:
        print("No table found.")
        return pd.DataFrame()

    headers, rows = extract_table_rows(table)
    data = [dict(zip(headers, row)) for row in rows if headers and len(row) == len(headers)]
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} rows → {output_path}")
    return df


def build_player_stats_url(season_id: int = SEASON_ID, page: int = 1) -> str:
    url = f"{BASE_URL}/sk/stats/players/{season_id}/{LEAGUE_SLUG}?StatsType=pim"
    if page > 1:
        url = f"{url}&page={page}"
    return url


def build_player_tab_url(profile_url: str, tab: str) -> str:
    return f"{profile_url.rstrip('/')}/{tab}"


def parse_player_leaderboard_table(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Parse one paginated player leaderboard page."""
    table = soup.select_one("table.table")
    if table is None:
        return []

    headers, rows = extract_table_rows(table)
    out: list[dict[str, Any]] = []

    for row in rows:
        if not headers or len(row) != len(headers):
            continue

        cells = table.select("tr")
        # Find the matching <tr> with td cells by rank/name to preserve link data.
        # The number of rows is small enough that this selector walk is fine.
        matching_tr = None
        for tr in cells:
            tds = tr.select("td")
            if not tds:
                continue
            td_text = [clean_whitespace(safe_text(td)) for td in tds]
            if td_text == row:
                matching_tr = tr
                break

        record: dict[str, Any] = {}

        for header, value in zip(headers, row):
            key = PLAYER_LIST_COLUMN_MAP.get(header)
            if key:
                record[key] = value

        if row:
            record["rank"] = row[0]

        if matching_tr is not None:
            name_link = matching_tr.select_one("td.column-FullName a[href]")
            if name_link is None:
                name_link = matching_tr.select_one("td:nth-of-type(2) a[href]")

            if name_link is not None:
                href = urljoin(BASE_URL, name_link.get("href", ""))
                record["profile_url"] = href
                record["player_id"] = extract_id_from_href(href, "player")
                record["player_slug"] = href.rstrip("/").split("/")[-1]

            team_abbr = matching_tr.select_one("td.column-TeamCode abbr")
            if team_abbr is not None:
                record["team"] = clean_whitespace(team_abbr.get("title", "")) or safe_text(team_abbr)
                record["team_code"] = safe_text(team_abbr)
            else:
                record["team"] = record.get("team_code", "")

        out.append(record)

    return out


def scrape_player_leaderboard(
    season_id: int = SEASON_ID,
    delay: float = 0.15,
) -> pd.DataFrame:
    """Scrape every paginated leaderboard page from the official skater stats page."""
    first_url = build_player_stats_url(season_id, page=1)
    first_soup = get_soup(first_url)
    if not first_soup:
        return pd.DataFrame()

    page_count = max_page_from_soup(first_soup)
    all_rows = parse_player_leaderboard_table(first_soup)
    print(f"Found {page_count} player leaderboard pages")

    for page in range(2, page_count + 1):
        url = build_player_stats_url(season_id, page=page)
        soup = get_soup(url)
        if soup is None:
            print(f"  Skipping player page {page} (fetch failed)")
            continue
        all_rows.extend(parse_player_leaderboard_table(soup))
        time.sleep(delay)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["player_id", "team_code"], keep="first")
    return df.reset_index(drop=True)


def parse_player_profile_bio(soup: BeautifulSoup) -> dict[str, Any]:
    """Extract bio details from the common player profile header panel."""
    panel = soup.select_one(".panel-player")
    if panel is None:
        return {}

    data: dict[str, Any] = {}

    title = panel.select_one("h2.tt-uppercase")
    if title is not None:
        title_text = clean_whitespace(title.get_text(" ", strip=True))
        data["player"] = re.sub(r"\s+#\s*\d+\s*$", "", title_text)

    jersey = panel.select_one(".player-number")
    if jersey is not None:
        match = re.search(r"(\d+)", safe_text(jersey))
        if match:
            data["jersey_number"] = match.group(1)

    team_link = panel.select_one("h3 a[href]")
    if team_link is not None:
        team_href = urljoin(BASE_URL, team_link.get("href", ""))
        data["team"] = safe_text(team_link)
        data["team_id"] = extract_id_from_href(team_href, "team")
        data["team_url"] = team_href

    position = safe_text(panel.select_one(".playa-position"))
    if not position:
        position = safe_text(panel.select_one(".playa-params.playa-position"))
        position = position.split(",")[0].strip()
    if position:
        data["position_full"] = position

    label_map: dict[str, str] = {}
    for info_block in panel.select(".player-data p"):
        parts = [clean_whitespace(x) for x in info_block.stripped_strings]
        if len(parts) >= 2:
            label_map[parts[0]] = parts[1]

    data["height"] = label_map.get("Výška", "")
    data["weight"] = label_map.get("Váha", "")
    data["shoots"] = label_map.get("Streľba", "")

    panel_text = clean_whitespace(panel.get_text(" ", strip=True))
    birth_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", panel_text)
    if birth_match:
        data["birth_date"] = birth_match.group(1)

    age_match = re.search(r"Age:\s*(\d+)", panel_text) or re.search(r"\b(\d+)\s+rokov\b", panel_text)
    if age_match:
        data["age"] = age_match.group(1)

    photo = panel.select_one(".player-photo")
    if photo is not None:
        style = photo.get("style", "")
        photo_match = re.search(r"url\('([^']+)'\)", style)
        if photo_match:
            data["photo_url"] = photo_match.group(1)

    return data


def parse_player_totals_table(soup: BeautifulSoup) -> dict[str, Any]:
    """Parse the 'Celkové' totals row from the official player Stats tab."""
    for table in soup.select("table.table"):
        headers, rows = extract_table_rows(table)
        if "PTS_AVG" not in headers or "GWG" not in headers:
            continue

        for row in rows:
            if not row or row[0] != "Celkové" or len(row) != len(headers):
                continue

            raw = dict(zip(headers, row))
            out: dict[str, Any] = {}
            for source, target in PLAYER_PROFILE_TOTAL_MAP.items():
                if source in raw:
                    out[target] = raw[source]
            return out

    return {}


def parse_player_gamelog_table(
    soup: BeautifulSoup,
    base_info: dict[str, Any],
) -> list[dict[str, Any]]:
    """Parse the official per-player game log table."""
    table = soup.select_one("table.table")
    if table is None:
        return []

    headers, rows = extract_table_rows(table)
    if not headers or "Súper" not in headers or "Dátum" not in headers:
        return []

    out: list[dict[str, Any]] = []

    for row in rows:
        if len(row) != len(headers):
            continue

        raw = dict(zip(headers, row))
        record = {
            "player_id": base_info.get("player_id"),
            "player": base_info.get("player"),
            "team": base_info.get("team"),
            "team_code": base_info.get("team_code"),
            "team_id": base_info.get("team_id"),
        }

        for source, target in PLAYER_GAMELOG_MAP.items():
            if source in raw:
                record[target] = raw[source]

        opponent_raw = record.get("opponent_raw", "")
        if opponent_raw.startswith("vs."):
            record["venue"] = "home"
            record["opponent"] = clean_whitespace(opponent_raw.replace("vs.", "", 1))
        elif opponent_raw.startswith("@"):
            record["venue"] = "away"
            record["opponent"] = clean_whitespace(opponent_raw.replace("@", "", 1))
        else:
            record["venue"] = ""
            record["opponent"] = opponent_raw

        out.append(record)

    return out


def finalize_players_dataframe(df: pd.DataFrame, season_id: int) -> pd.DataFrame:
    """Coerce numeric fields and add useful rate stats for dashboard/model use."""
    if df.empty:
        return df

    df = df.copy()
    df["season"] = season_id
    df["league"] = "slovakia"

    numeric_cols = [
        "rank", "player_id", "team_id", "birth_year", "age", "jersey_number",
        "games", "goals", "assists", "points", "plus_minus", "pim",
        "penalties_2", "penalties_5", "penalties_10", "penalties_20", "penalties_25",
        "even_strength_points", "power_play_points", "shorthanded_points",
        "shots", "game_winning_goals", "game_tying_goals",
        "faceoffs_taken", "faceoff_win_pct", "hits",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].map(parse_numeric)

    if "birth_date" in df.columns:
        df["birth_date"] = df["birth_date"].fillna("").map(parse_birth_date)

    if "height" in df.columns:
        df["height_cm"] = df["height"].str.extract(r"(\d+)")[0].map(parse_numeric)
    if "weight" in df.columns:
        df["weight_kg"] = df["weight"].str.extract(r"(\d+)")[0].map(parse_numeric)

    if "toi" in df.columns:
        df["toi_seconds"] = df["toi"].fillna("").map(parse_time_to_seconds)
        df["toi_minutes"] = df["toi_seconds"] / 60.0

    games = pd.to_numeric(df.get("games"), errors="coerce")
    for source, target in [
        ("goals", "goals_per_game"),
        ("assists", "assists_per_game"),
        ("points", "points_per_game_derived"),
        ("shots", "shots_per_game"),
        ("pim", "pim_per_game"),
    ]:
        if source in df.columns:
            df[target] = pd.to_numeric(df[source], errors="coerce") / games.replace(0, pd.NA)

    if "toi_seconds" in df.columns:
        df["toi_per_game_minutes"] = (df["toi_seconds"] / games.replace(0, pd.NA)) / 60.0

    if "points_per_game" not in df.columns and "points_per_game_derived" in df.columns:
        df["points_per_game"] = df["points_per_game_derived"]

    preferred_cols = [
        "season", "league", "player_id", "player", "team", "team_code", "team_id",
        "position", "position_full", "birth_year", "birth_date", "age", "shoots",
        "height_cm", "weight_kg", "jersey_number", "games", "goals", "assists",
        "points", "goals_per_game", "assists_per_game", "points_per_game",
        "plus_minus", "pim", "pim_per_game", "shots", "shots_per_game",
        "even_strength_points", "power_play_points", "shorthanded_points",
        "game_winning_goals", "game_tying_goals", "faceoffs_taken", "faceoff_win_pct",
        "hits", "toi", "toi_seconds", "toi_minutes", "toi_per_game_minutes",
        "penalties_2", "penalties_5", "penalties_10", "penalties_20", "penalties_25",
        "profile_url", "photo_url", "team_url",
    ]

    existing = [col for col in preferred_cols if col in df.columns]
    remaining = [col for col in df.columns if col not in existing]
    df = df[existing + remaining]

    sort_cols = [col for col in ["points", "points_per_game", "goals", "assists"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))

    return df.reset_index(drop=True)


def finalize_player_gamelogs_dataframe(df: pd.DataFrame, season_id: int) -> pd.DataFrame:
    """Coerce game-log columns into a clean analysis-ready table."""
    if df.empty:
        return df

    df = df.copy()
    df["season"] = season_id
    df["league"] = "slovakia"

    numeric_cols = [
        "player_id", "team_id", "goals", "assists", "points", "plus_minus",
        "pim", "shots", "game_winning_goals", "game_tying_goals",
        "faceoffs_taken", "faceoff_win_pct", "hits",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].map(parse_numeric)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")

    if "toi" in df.columns:
        df["toi_seconds"] = df["toi"].fillna("").map(parse_time_to_seconds)
        df["toi_minutes"] = df["toi_seconds"] / 60.0

    preferred_cols = [
        "season", "league", "date", "player_id", "player", "team", "team_code",
        "team_id", "venue", "opponent", "result", "goals", "assists", "points",
        "plus_minus", "pim", "shots", "game_winning_goals", "game_tying_goals",
        "faceoffs_taken", "faceoff_win_pct", "hits", "toi", "toi_seconds",
        "toi_minutes", "opponent_raw",
    ]
    existing = [col for col in preferred_cols if col in df.columns]
    remaining = [col for col in df.columns if col not in existing]
    return df[existing + remaining].reset_index(drop=True)


def scrape_player_stats(
    season_id: int = SEASON_ID,
    delay: float = 0.15,
    limit: int | None = None,
    with_gamelogs: bool = False,
) -> pd.DataFrame:
    """
    Scrape official Slovakia skater data:
    - paginated season leaderboard
    - per-player profile totals and bio fields
    - optional per-player official game logs
    """
    leaderboard_df = scrape_player_leaderboard(season_id=season_id, delay=delay)
    if leaderboard_df.empty:
        print("No player leaderboard data found.")
        return leaderboard_df

    if limit is not None:
        leaderboard_df = leaderboard_df.head(limit).copy()

    enriched_rows: list[dict[str, Any]] = []
    gamelog_rows: list[dict[str, Any]] = []

    for idx, row in leaderboard_df.reset_index(drop=True).iterrows():
        base = row.to_dict()
        profile_url = base.get("profile_url", "")
        stats_url = build_player_tab_url(profile_url, "Stats") if profile_url else ""
        gamelog_url = build_player_tab_url(profile_url, "GameLog") if profile_url else ""

        if (idx + 1) % 25 == 0 or idx == 0:
            print(f"  Enriching player {idx + 1}/{len(leaderboard_df)}")

        if stats_url:
            soup = get_soup(stats_url)
            if soup is not None:
                base.update(parse_player_profile_bio(soup))
                base.update(parse_player_totals_table(soup))

        enriched_rows.append(base)

        if with_gamelogs and gamelog_url:
            soup = get_soup(gamelog_url)
            if soup is not None:
                gamelog_rows.extend(parse_player_gamelog_table(soup, base))

        time.sleep(delay)

    players_df = finalize_players_dataframe(pd.DataFrame(enriched_rows), season_id=season_id)

    season_path = f"{OUTPUT_DIR}/season_{season_id}_players.csv"
    players_path = f"{OUTPUT_DIR}/players.csv"
    leaderboard_path = f"{OUTPUT_DIR}/season_{season_id}_players_leaderboard.csv"

    leaderboard_df.to_csv(leaderboard_path, index=False, encoding="utf-8-sig")
    players_df.to_csv(season_path, index=False, encoding="utf-8-sig")
    players_df.to_csv(players_path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(leaderboard_df)} leaderboard rows → {leaderboard_path}")
    print(f"Saved {len(players_df)} enriched players → {season_path}")
    print(f"Saved {len(players_df)} enriched players → {players_path}")

    if with_gamelogs:
        gamelog_df = finalize_player_gamelogs_dataframe(pd.DataFrame(gamelog_rows), season_id=season_id)
        gamelog_path = f"{OUTPUT_DIR}/players_game_log.csv"
        season_gamelog_path = f"{OUTPUT_DIR}/season_{season_id}_players_game_log.csv"
        gamelog_df.to_csv(gamelog_path, index=False, encoding="utf-8-sig")
        gamelog_df.to_csv(season_gamelog_path, index=False, encoding="utf-8-sig")
        print(f"Saved {len(gamelog_df)} player game logs → {gamelog_path}")
        print(f"Saved {len(gamelog_df)} player game logs → {season_gamelog_path}")

    return players_df


def scrape_goalie_stats(season_id: int = SEASON_ID) -> pd.DataFrame:
    """Scrape the official goalie leaderboard page when available."""
    url = f"{BASE_URL}/sk/stats/goalies/{season_id}/{LEAGUE_SLUG}?StatsType=svs"
    path = f"{OUTPUT_DIR}/season_{season_id}_goalies.csv"
    return scrape_generic_stats_table(url, path)


# ── Diagnosis tool ────────────────────────────────────────────────────────────

def diagnose_match(match_id: int = 153021, season_id: int = SEASON_ID) -> None:
    """
    Print useful diagnostics from a match page so selectors can be adjusted.
    """
    url = (
        f"{BASE_URL}/sk/stats/matches/{season_id}/{LEAGUE_SLUG}"
        f"/match/{match_id}/Overview"
    )
    print(f"Fetching: {url}\n")

    soup = get_soup(url)
    if not soup:
        print("Failed to fetch page — site may be blocking requests or be temporarily down.")
        return

    print("=== Page title ===")
    print(soup.title.string if soup.title else "(no title)")

    print("\n=== Score block ===")
    score_el = soup.select_one("span.score, .match-score, .score-result, .final-score")
    print(safe_text(score_el) if score_el else "(not found)")

    print("\n=== Period score block ===")
    period_el = soup.select_one("span.scoreperiod, .scoreperiod, .period-score, .periods")
    print(safe_text(period_el) if period_el else "(not found)")

    print("\n=== Teams from title ===")
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    home_team, away_team = extract_teams_from_title(title_text)
    print(f"Home: {home_team}")
    print(f"Away: {away_team}")

    print("\n=== All one-row event tables found ===")
    event_tables = extract_event_tables(soup)
    for i, text in enumerate(event_tables[:30]):
        print(f"  Event {i}: {text[:150]}")
    if len(event_tables) > 30:
        print(f"  ... and {len(event_tables) - 30} more")

    print("\n=== Raw HTML around score ===")
    if score_el and score_el.parent:
        print(score_el.parent.prettify()[:2000])
    else:
        print("(not found)")

    print("\n=== Sample of page text (first 1200 chars) ===")
    print(soup.get_text(separator=" ", strip=True)[:1200])


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="hockeyslovakia.sk scraper")
    parser.add_argument("--match", type=int, help="Scrape a single match ID")
    parser.add_argument("--season", action="store_true", help="Scrape full season")
    parser.add_argument("--players", action="store_true", help="Scrape rich official player stats")
    parser.add_argument("--goalies", action="store_true", help="Scrape goalie stats")
    parser.add_argument("--with-gamelogs", action="store_true", help="Also scrape official player game logs")
    parser.add_argument("--diagnose", action="store_true", help="Diagnose page structure")
    parser.add_argument("--limit", type=int, help="Limit season scrape to N matches")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (s)")
    args = parser.parse_args()

    if args.diagnose:
        match_id = args.match or 153021
        diagnose_match(match_id)

    elif args.match:
        data = scrape_match(args.match)
        print(json.dumps(data, indent=2, ensure_ascii=False))

        path = f"{OUTPUT_DIR}/match_{args.match}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"\nSaved → {path}")

    elif args.season:
        df = scrape_season(limit=args.limit, delay=args.delay)
        print(df.head().to_string(index=False))

    elif args.players:
        df = scrape_player_stats(
            delay=args.delay,
            limit=args.limit,
            with_gamelogs=args.with_gamelogs,
        )
        print(df.head(10).to_string(index=False))

    elif args.goalies:
        df = scrape_goalie_stats()
        print(df.head(10).to_string(index=False))

    else:
        print("No argument given — running diagnosis on match 153021.")
        print("Run with --help to see all options.\n")
        diagnose_match(153021)


if __name__ == "__main__":
    main()
