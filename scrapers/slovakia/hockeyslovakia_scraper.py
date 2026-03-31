"""
hockeyslovakia.sk scraper
Scrapes match data from the Tipsport Liga (Slovak Extraliga)

Usage:
    python hockeyslovakia_scraper.py                  # diagnose default match
    python hockeyslovakia_scraper.py --season         # scrape full season
    python hockeyslovakia_scraper.py --match 153021   # specific match ID
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

def scrape_generic_stats_table(url: str, output_path: str) -> pd.DataFrame:
    """
    Generic helper for player/goalie leaderboard pages.
    """
    print(f"Fetching stats from: {url}")
    soup = get_soup(url)
    if not soup:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []

    table = soup.select_one("table.stats-table, table#players-stats, .players-stats table")
    if table is None:
        tables = soup.select("table")
        if tables:
            table = max(tables, key=lambda t: len(t.select("tr")))

    if table is None:
        print("No table found.")
        return pd.DataFrame()

    headers = [clean_whitespace(safe_text(th)) for th in table.select("th")]

    for row in table.select("tr"):
        cols = [clean_whitespace(safe_text(td)) for td in row.select("td")]
        if not cols:
            continue

        if headers and len(cols) == len(headers):
            rows.append(dict(zip(headers, cols)))
        else:
            rows.append({"raw": " | ".join(cols)})

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} rows → {output_path}")

    return df


def scrape_player_stats(season_id: int = SEASON_ID) -> pd.DataFrame:
    """Scrape the full season player stats leaderboard."""
    url = f"{BASE_URL}/sk/stats/players/{season_id}/{LEAGUE_SLUG}"
    path = f"{OUTPUT_DIR}/season_{season_id}_players.csv"
    return scrape_generic_stats_table(url, path)


def scrape_goalie_stats(season_id: int = SEASON_ID) -> pd.DataFrame:
    """Scrape the full season goalie stats leaderboard."""
    url = f"{BASE_URL}/sk/stats/goalies/{season_id}/{LEAGUE_SLUG}"
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
    parser.add_argument("--players", action="store_true", help="Scrape player stats")
    parser.add_argument("--goalies", action="store_true", help="Scrape goalie stats")
    parser.add_argument("--diagnose", action="store_true", help="Diagnose page structure")
    parser.add_argument("--limit", type=int, help="Limit season scrape to N matches")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests (s)")
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
        df = scrape_player_stats()
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