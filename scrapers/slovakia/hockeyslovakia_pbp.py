from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.hockeyslovakia.sk"
SEASON_ID = 1131          # 2025-26 season
LEAGUE_SLUG = "tipsport-liga"
OUTPUT_DIR = str(Path(__file__).parent / "data" / "input")

KNOWN_TEAM_FRAGMENTS = [
    "slovan", "bratislava",
    "košice", "kosice",
    "poprad",
    "nitra",
    "zvolen",
    "banská bystrica", "banska bystrica",
    "prešov", "presov",
    "michalovce", "dukla",
    "liptovský mikuláš", "liptovsky mikulas", "lm",
    "trenčín", "trencin",
    "žilina", "zilina", "vlci",
    "spišská nová ves", "spisska nova ves",
]

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_soup(url: str) -> BeautifulSoup | None:
    try:
        response = SESSION.get(url, timeout=20)
        if response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")
        print(f"  HTTP {response.status_code} for {url}")
        return None
    except requests.RequestException as exc:
        print(f"  Request error for {url}: {exc}")
        return None


def clean_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def safe_text(el: Any) -> str:
    return clean_whitespace(el.get_text(" ", strip=True)) if el else ""


def build_match_url(match_id: int, season_id: int = SEASON_ID, page: str = "PlayByPlay") -> str:
    return (
        f"{BASE_URL}/sk/stats/matches/{season_id}/{LEAGUE_SLUG}"
        f"/match/{match_id}/{page}"
    )


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
    nums = re.findall(r"\d+", score_text)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    return None, None


def parse_date_from_text(text: str) -> pd.Timestamp:
    if not text:
        return pd.NaT

    text = clean_whitespace(text)

    patterns = [
        (r"\b(\d{1,2}\.\d{1,2}\.\d{4})\b", "%d.%m.%Y"),
        (r"\b(\d{4}-\d{2}-\d{2})\b", "%Y-%m-%d"),
        (r"\b(\d{1,2}/\d{1,2}/\d{4})\b", "%d/%m/%Y"),
    ]

    for pattern, fmt in patterns:
        m = re.search(pattern, text)
        if m:
            try:
                return pd.Timestamp(datetime.strptime(m.group(1), fmt))
            except ValueError:
                pass

    return pd.NaT


def to_seconds(period: str | None, time_str: str | None) -> int | None:
    """Convert period + MM:SS to elapsed game seconds (20-min periods)."""
    if not period or not time_str:
        return None
    period_map = {"P1": 0, "P2": 1, "P3": 2, "OT": 3, "SO": 4}
    if period not in period_map:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})$", time_str)
    if not m:
        return None
    minutes, seconds = int(m.group(1)), int(m.group(2))
    return period_map[period] * 20 * 60 + minutes * 60 + seconds


def _is_team_line(line: str, home: str, away: str) -> bool:
    low = line.lower()

    if home and home.lower() in low:
        return True
    if away and away.lower() in low:
        return True

    return any(frag in low for frag in KNOWN_TEAM_FRAGMENTS)


# ── Match ID discovery ────────────────────────────────────────────────────────

def _extract_match_ids_from_html(html: str, season_id: int) -> list[int]:
    pattern = re.compile(
        rf"/sk/stats/matches/{season_id}/{LEAGUE_SLUG}/match/(\d+)(?:/|\"|'|\?)"
    )
    return sorted({int(x) for x in pattern.findall(html or "")})


def get_match_ids_from_pages(season_id: int = SEASON_ID) -> list[int]:
    """
    Try several likely season/result pages and collect all match IDs.
    """
    candidate_urls = [
        f"{BASE_URL}/sk/stats/results-date/{season_id}/{LEAGUE_SLUG}",
        f"{BASE_URL}/sk/stats/matches/{season_id}/{LEAGUE_SLUG}",
        f"{BASE_URL}/sk/stats/schedule/{season_id}/{LEAGUE_SLUG}",
        f"{BASE_URL}/sk/stats/results/{season_id}/{LEAGUE_SLUG}",
    ]

    ids: set[int] = set()

    for url in candidate_urls:
        try:
            response = SESSION.get(url, timeout=20)
        except requests.RequestException as exc:
            print(f"  Request error for {url}: {exc}")
            continue

        if response.status_code != 200:
            print(f"  HTTP {response.status_code} for {url}")
            continue

        page_ids = _extract_match_ids_from_html(response.text, season_id)
        if page_ids:
            print(f"  Found {len(page_ids)} IDs on {url}")
            ids.update(page_ids)

    return sorted(ids)


def match_exists(match_id: int, season_id: int = SEASON_ID, page: str = "Overview") -> bool:
    """
    Check whether a match page exists.
    Overview is usually more reliable than PlayByPlay for discovery.
    """
    url = build_match_url(match_id, season_id=season_id, page=page)
    try:
        response = SESSION.get(url, timeout=10)
    except requests.RequestException:
        return False

    if response.status_code != 200:
        return False

    text = response.text
    return (
        f"/match/{match_id}/" in text
        or " vs. " in text
        or "Číslo zápasu" in text
        or "Herné situácie" in text
    )


def adaptive_match_id_scan(
    season_id: int = SEASON_ID,
    seed_ids: list[int] | None = None,
    center_match_id: int = 153021,
    probe_delay: float = 0.10,
    max_consecutive_misses: int = 250,
) -> list[int]:
    """
    Expand outward from known IDs (or center_match_id) until a long run of misses
    is reached on both sides.
    """
    if seed_ids:
        low = min(seed_ids)
        high = max(seed_ids)
    else:
        low = high = center_match_id

    found = set(seed_ids or [])

    print(f"Adaptive scan starting from low={low}, high={high}")

    misses = 0
    mid = low - 1
    while misses < max_consecutive_misses and mid > 0:
        if match_exists(mid, season_id=season_id, page="Overview"):
            found.add(mid)
            print(f"  Found match {mid} (down)")
            misses = 0
        else:
            misses += 1
        mid -= 1
        time.sleep(probe_delay)

    misses = 0
    mid = high + 1
    while misses < max_consecutive_misses:
        if match_exists(mid, season_id=season_id, page="Overview"):
            found.add(mid)
            print(f"  Found match {mid} (up)")
            misses = 0
        else:
            misses += 1
        mid += 1
        time.sleep(probe_delay)

    return sorted(found)


def get_match_ids(
    season_id: int = SEASON_ID,
    center_match_id: int = 153021,
    window: int = 400,
    probe_delay: float = 0.15,
) -> list[int]:
    print("Trying season/result pages for match IDs …")
    ids = get_match_ids_from_pages(season_id)

    if len(ids) >= 100:
        print(f"  Found {len(ids)} matches from page parsing")
        return ids

    print(f"  Page parsing found only {len(ids)} matches; switching to adaptive scan …")
    ids = adaptive_match_id_scan(
        season_id=season_id,
        seed_ids=ids,
        center_match_id=center_match_id,
        probe_delay=probe_delay,
        max_consecutive_misses=250,
    )

    print(f"  Final discovered matches: {len(ids)}")
    return ids


# ── Header parsing ────────────────────────────────────────────────────────────

def parse_match_header(soup: BeautifulSoup) -> dict[str, Any]:
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    home_team, away_team = extract_teams_from_title(title_text)

    score_text = ""
    home_score: int | None = None
    away_score: int | None = None
    match_date = pd.NaT

    for selector in (
        "span.score", ".score", ".result", ".match-result",
        "td.score", "div.score", ".scoreboard",
    ):
        el = soup.select_one(selector)
        if el:
            score_text = safe_text(el)
            home_score, away_score = parse_score(score_text)
            if home_score is not None:
                break

    if home_score is None:
        page_text = soup.get_text(" ", strip=True)
        for m in re.finditer(r"\b(\d{1,2}):(\d{1,2})\b", page_text):
            h, a = int(m.group(1)), int(m.group(2))
            if h <= 20 and a <= 20:
                home_score, away_score = h, a
                score_text = m.group(0)
                break

    period_el = soup.select_one("span.scoreperiod, .scoreperiod, .period-scores")
    periods_text = safe_text(period_el) if period_el else ""
    if not periods_text:
        m = re.search(r"\((\d+:\d+(?:[,\s]+\d+:\d+)*)\)", soup.get_text(" "))
        if m:
            periods_text = m.group(1)

    page_text = soup.get_text("\n", strip=True)
    lines = [clean_whitespace(x) for x in page_text.splitlines() if clean_whitespace(x)]
    meta_line = next((line for line in lines if "Číslo zápasu" in line), "")

    date_selectors = [
        ".match-date",
        ".date",
        "time",
        ".game-date",
        ".fixture-date",
        ".matchInfo__date",
    ]
    for selector in date_selectors:
        el = soup.select_one(selector)
        if el:
            match_date = parse_date_from_text(safe_text(el))
            if pd.notna(match_date):
                break

    if pd.isna(match_date):
        match_date = parse_date_from_text(meta_line)

    if pd.isna(match_date):
        full_text = soup.get_text(" ", strip=True)
        match_date = parse_date_from_text(full_text)

    return {
        "title": title_text,
        "date": match_date,
        "home_team": home_team,
        "away_team": away_team,
        "score": score_text,
        "home_score": home_score,
        "away_score": away_score,
        "period_scores": " | ".join(re.findall(r"\d+:\d+", periods_text)),
        "meta_line": meta_line,
    }


# ── Play-by-play parsing ──────────────────────────────────────────────────────

def extract_playbyplay_lines(soup: BeautifulSoup) -> list[str]:
    lines = [
        clean_whitespace(x)
        for x in soup.get_text("\n", strip=True).splitlines()
    ]
    lines = [x for x in lines if x]
    try:
        start = lines.index("Herné situácie")
        return lines[start:]
    except ValueError:
        return lines


def parse_period_start(line: str) -> tuple[str | None, str | None]:
    m = re.match(
        r"^(P[123]|OT|SO)\s*-\s*.+?-\s*začiatok(?:\s*\((\d+:\d+)\))?",
        line,
    )
    if m:
        return m.group(1), m.group(2)
    return None, None


def classify_event_block(block: list[str]) -> str:
    joined = " ".join(block).lower()
    if any(re.search(r"\d+\s*min\.", x) for x in block):
        return "penalty"
    if any(re.match(r"^\d+:\d+(?:\s+\w+)?$", x) for x in block[1:]):
        return "goal"
    if "vhadz" in joined:
        return "faceoff"
    if "strela" in joined:
        return "shot"
    return "other"


def _empty_event_fields() -> dict[str, Any]:
    return {
        "team": "",
        "player": "",
        "secondary_player_1": "",
        "secondary_player_2": "",
        "score_state": "",
        "goal_type": "",
        "penalty_minutes": None,
        "penalty_reason": "",
        "raw_block": "",
    }



def _is_shift_marker(line: str) -> bool:
    return (
        line in {",", "/", "+", "-"}
        or line.endswith(";")
        or line.startswith("+ ")
        or line.startswith("- ")
    )


def parse_penalty_block(
    block: list[str],
    home_team: str = "",
    away_team: str = "",
) -> dict[str, Any]:
    out = _empty_event_fields()
    out["raw_block"] = " | ".join(block)

    if len(block) >= 2 and re.match(r"^\d+\s*min\.$", block[1]):
        out["penalty_minutes"] = int(re.findall(r"\d+", block[1])[0])

    if len(block) >= 3:
        out["team"] = block[2]

    if len(block) >= 4:
        candidate = block[3]
        if candidate.lower() != "team penalty":
            out["player"] = candidate

    reason = next((x for x in block if x.startswith("(") and x.endswith(")")), "")
    out["penalty_reason"] = reason.strip("()")

    return out


def parse_goal_block(
    block: list[str],
    home_team: str = "",
    away_team: str = "",
) -> dict[str, Any]:
    out = _empty_event_fields()
    out["raw_block"] = " | ".join(block)

    if len(block) >= 2:
        score_line = block[1]
        m = re.match(r"^(\d+:\d+)(?:\s+(\w+))?$", score_line)
        if m:
            out["score_state"] = m.group(1)
            out["goal_type"] = m.group(2) or ""

    if len(block) >= 3:
        out["team"] = block[2]

    names: list[str] = []
    for idx, line in enumerate(block[3:], start=3):
        if not line or _is_shift_marker(line):
            continue
        if line.startswith("(") and line.endswith(")"):
            continue
        if re.match(r"^\d{1,2}:\d{2}$", line):
            continue
        if re.match(r"^\d+:\d+(?:\s+\w+)?$", line):
            continue
        if line == out["team"]:
            continue
        names.append(line)

    if names:
        out["player"] = names[0]
    if len(names) > 1:
        out["secondary_player_1"] = names[1]
    if len(names) > 2:
        out["secondary_player_2"] = names[2]

    return out


def parse_other_block(block: list[str]) -> dict[str, Any]:
    out = _empty_event_fields()
    out["raw_block"] = " | ".join(block)
    return out


# ── Per-match scraper ─────────────────────────────────────────────────────────

def parse_playbyplay_page(
    match_id: int,
    season_id: int = SEASON_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    url = build_match_url(match_id, season_id=season_id, page="PlayByPlay")
    print(f"  Scraping play-by-play for match {match_id} …")

    soup = get_soup(url)
    if not soup:
        return {"match_id": match_id, "url": url, "error": "fetch_failed"}, []

    header = parse_match_header(soup)
    header["match_id"] = match_id
    header["url"] = url
    header["error"] = ""

    home_team = header.get("home_team", "")
    away_team = header.get("away_team", "")

    lines = extract_playbyplay_lines(soup)

    events: list[dict[str, Any]] = []
    current_period = None
    current_period_start_score = None
    event_idx = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        period_code, period_score = parse_period_start(line)
        if period_code:
            current_period = period_code
            current_period_start_score = period_score
            i += 1
            continue

        if re.match(r"^\d{1,2}:\d{2}$", line):
            time_str = line
            block = [time_str]
            i += 1

            while i < len(lines):
                next_line = lines[i]
                if parse_period_start(next_line)[0] is not None:
                    break
                if re.match(r"^\d{1,2}:\d{2}$", next_line):
                    break
                if next_line == "Zápas sa skončil":
                    break
                block.append(next_line)
                i += 1

            event_type = classify_event_block(block)

            if event_type == "penalty":
                parsed = parse_penalty_block(block, home_team, away_team)
            elif event_type == "goal":
                parsed = parse_goal_block(block, home_team, away_team)
            else:
                parsed = parse_other_block(block)

            events.append({
                "match_id": match_id,
                "event_idx": event_idx,
                "period": current_period,
                "period_start_score": current_period_start_score,
                "time": time_str,
                "elapsed_seconds": to_seconds(current_period, time_str),
                "event_type": event_type,
                **parsed,
            })
            event_idx += 1
            continue

        if line == "Zápas sa skončil":
            break

        i += 1

    return header, events


# ── Season scraper ────────────────────────────────────────────────────────────

def scrape_season_playbyplay(
    season_id: int = SEASON_ID,
    center_match_id: int = 153021,
    window: int = 400,
    probe_delay: float = 0.15,
    scrape_delay: float = 1.0,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    match_ids = get_match_ids(
        season_id=season_id,
        center_match_id=center_match_id,
        window=window,
        probe_delay=probe_delay,
    )

    if not match_ids:
        raise RuntimeError("No match IDs found.")

    if limit is not None:
        match_ids = match_ids[:limit]

    match_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []

    for idx, match_id in enumerate(match_ids):
        header, events = parse_playbyplay_page(match_id, season_id=season_id)
        match_rows.append(header)
        event_rows.extend(events)
        if idx < len(match_ids) - 1:
            time.sleep(scrape_delay)

    matches_df = pd.DataFrame(match_rows)
    matches_df["date"] = pd.to_datetime(matches_df["date"], errors="coerce")
    matches_df = matches_df[matches_df["date"] <= pd.Timestamp.today().normalize()]
    events_df = pd.DataFrame(event_rows)
    events_df = events_df[events_df["match_id"].isin(matches_df["match_id"])]

    goal_events = events_df[events_df["event_type"] == "goal"].copy()
    penalty_events = events_df[events_df["event_type"] == "penalty"].copy()

    goal_scorers = (
        goal_events.loc[goal_events["player"].fillna("").ne(""), ["player", "team"]]
        .assign(goals=1)
        .groupby(["player", "team"], as_index=False)["goals"]
        .sum()
    )

    assists_1 = (
        goal_events.loc[goal_events["secondary_player_1"].fillna("").ne(""), ["secondary_player_1", "team"]]
        .rename(columns={"secondary_player_1": "player"})
        .assign(assists=1)
        .groupby(["player", "team"], as_index=False)["assists"]
        .sum()
    )

    assists_2 = (
        goal_events.loc[goal_events["secondary_player_2"].fillna("").ne(""), ["secondary_player_2", "team"]]
        .rename(columns={"secondary_player_2": "player"})
        .assign(assists=1)
        .groupby(["player", "team"], as_index=False)["assists"]
        .sum()
    )

    penalties = (
        penalty_events.loc[penalty_events["player"].fillna("").ne(""), ["player", "team", "penalty_minutes"]]
        .assign(
            penalties=1,
            pim=lambda df: pd.to_numeric(df["penalty_minutes"], errors="coerce").fillna(0)
        )
        .groupby(["player", "team"], as_index=False)[["penalties", "pim"]]
        .sum()
    )

    players_df = goal_scorers.merge(assists_1, on=["player", "team"], how="outer")
    players_df = players_df.merge(assists_2, on=["player", "team"], how="outer", suffixes=("", "_2"))
    players_df = players_df.merge(penalties, on=["player", "team"], how="outer")

    for col in ["goals", "assists", "assists_2", "penalties", "pim"]:
        if col in players_df.columns:
            players_df[col] = players_df[col].fillna(0)

    if "assists_2" in players_df.columns:
        players_df["assists"] = players_df["assists"] + players_df["assists_2"]
        players_df = players_df.drop(columns=["assists_2"])

    for col in ["goals", "assists", "penalties", "pim"]:
        if col not in players_df.columns:
            players_df[col] = 0

    players_df["points"] = players_df["goals"] + players_df["assists"]

    players_df = players_df.sort_values(
        ["points", "goals", "assists", "pim"],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)


    matches_path = f"{OUTPUT_DIR}/games.csv"
    events_path = f"{OUTPUT_DIR}/events.csv"
    players_path = f"{OUTPUT_DIR}/players.csv"

    matches_df.to_csv(matches_path, index=False, encoding="utf-8-sig")
    events_df.to_csv(events_path, index=False, encoding="utf-8-sig")
    players_df.to_csv(players_path, index=False, encoding="utf-8-sig")

    print(f"\nSaved {len(matches_df)} matches → {matches_path}")
    print(f"Saved {len(events_df)} events  → {events_path}")
    print(f"Saved {len(players_df)} players → {players_path}")

    return matches_df, events_df, players_df


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Tipsport liga play-by-play scraper")
    parser.add_argument("--match", type=int, help="Scrape one match by ID")
    parser.add_argument("--season-pbp", action="store_true", help="Scrape full season")
    parser.add_argument("--season-id", type=int, default=SEASON_ID)
    parser.add_argument("--center-match-id", type=int, default=153021)
    parser.add_argument("--window", type=int, default=400)
    parser.add_argument("--probe-delay", type=float, default=0.15)
    parser.add_argument("--scrape-delay", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    if args.match:
        header, events = parse_playbyplay_page(args.match, season_id=args.season_id)
        out = {"header": header, "events": events}
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        path = f"{OUTPUT_DIR}/match_{args.match}_pbp.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False, default=str)
        print(f"\nSaved → {path}")

    elif args.season_pbp:
        matches_df, events_df, players_df = scrape_season_playbyplay(
            season_id=args.season_id,
            center_match_id=args.center_match_id,
            window=args.window,
            probe_delay=args.probe_delay,
            scrape_delay=args.scrape_delay,
            limit=args.limit,
        )
        print(matches_df.head().to_string(index=False))
        print(events_df.head().to_string(index=False))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()