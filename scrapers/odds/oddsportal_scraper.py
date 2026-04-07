#!/usr/bin/env python3
"""
OddsPortal scraper for European hockey leagues.

Uses Playwright (headless Chromium) to render OddsPortal's React pages.

Two modes:
  --fast   (default) Reads listing pages only. Extracts OddsPortal's average
           1X2 odds without visiting individual match pages. Very fast.
  --full   Also visits each match page to collect per-bookmaker odds.
           Much slower (1-2 s per match).

Requirements:
    pip install playwright beautifulsoup4
    playwright install chromium

Usage:
    python scrapers/odds/oddsportal_scraper.py
    python scrapers/odds/oddsportal_scraper.py --mode results
    python scrapers/odds/oddsportal_scraper.py --mode upcoming
    python scrapers/odds/oddsportal_scraper.py --leagues germany finland --pages 5
    python scrapers/odds/oddsportal_scraper.py --full              # per-bookmaker odds
    python scrapers/odds/oddsportal_scraper.py --full --max-games 20

Outputs (appended, deduped):
    scrapers/odds/data/input/oddsportal_games.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout
except ImportError:
    print("Missing dep. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dep. Run: pip install beautifulsoup4")
    sys.exit(1)


OUTPUT_DIR = Path(__file__).parent / "data" / "input"
OUTPUT_FILE = OUTPUT_DIR / "oddsportal_games.csv"
BASE_URL = "https://www.oddsportal.com"

LEAGUE_MAP: dict[str, dict[str, str]] = {
    "germany":     {"country": "germany",        "slug": "del",                "label": "DEL"},
    "finland":     {"country": "finland",         "slug": "liiga",              "label": "Liiga"},
    "sweden":      {"country": "sweden",          "slug": "shl",                "label": "SHL"},
    "czech":       {"country": "czech-republic",  "slug": "extraliga",          "label": "Czech Extraliga"},
    "austria":     {"country": "austria",         "slug": "ice-hockey-league",  "label": "ICE Hockey League"},
    "slovakia":    {"country": "slovakia",        "slug": "extraliga",          "label": "Slovak Extraliga"},
    "switzerland": {"country": "switzerland",     "slug": "national-league",    "label": "National League"},
    "khl":         {"country": "russia",          "slug": "khl",                "label": "KHL"},
}

OUTPUT_FIELDS = [
    "source", "league", "league_label",
    "game_url",
    "match_date",
    "home_team", "away_team",
    "home_score", "away_score",
    "status",
    "bookmaker",
    "outcome_type",   # home | draw | away
    "european_odds",
    "fetched_at",
]

PAGE_DELAY  = 2.5
MATCH_DELAY = 1.8

BROWSER_ARGS = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ── Browser ───────────────────────────────────────────────────────────────────

def make_page(playwright) -> tuple[Any, Page]:
    browser = playwright.chromium.launch(headless=True, args=BROWSER_ARGS)
    ctx = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()
    # Block images/fonts to speed up loading
    page.route(
        re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|woff2?|ttf|mp4)$"),
        lambda route: route.abort(),
    )
    return browser, page


def goto_wait(page: Page, url: str, timeout: int = 25000) -> str:
    """Navigate and wait for networkidle; return page HTML."""
    page.goto(url, wait_until="networkidle", timeout=timeout)
    page.wait_for_timeout(1500)
    return page.content()


# ── Listing page parsing ──────────────────────────────────────────────────────

def _parse_date(text: str) -> str:
    """Extract YYYY-MM-DD from strings like '04 Apr 2026  - Play Offs'."""
    m = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", text)
    if not m:
        return ""
    months = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    }
    d, mon, y = m.group(1).zfill(2), m.group(2), m.group(3)
    return f"{y}-{months.get(mon, '00')}-{d}"


def parse_event_rows(html: str, league: str, label: str, fetched_at: str) -> list[dict]:
    """
    Parse eventRow divs from a league listing/results page.

    Each eventRow text looks like one of:
      [date_header chunks] | status | abbr | Home Team | score | score | – | score | Away Team | score | h_odds | d_odds | a_odds
      status | abbr | Home Team | score | ... | Away Team | score | h_odds | d_odds | a_odds
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all(class_=re.compile(r"\beventRow\b"))
    records: list[dict] = []
    current_date = ""

    for row in rows:
        # h2h link gives us the match URL
        links = row.find_all("a", href=True)
        h2h_link = next((a for a in links if "/h2h/" in a["href"]), None)
        if not h2h_link:
            continue

        href = h2h_link["href"].split("#")[0].rstrip("/")
        match_id = h2h_link["href"].split("#")[-1] if "#" in h2h_link["href"] else ""
        game_url = BASE_URL + href + (f"#{match_id}" if match_id else "")

        # Full text of the row, pipe-separated
        parts = [p.strip() for p in row.get_text(separator="|", strip=True).split("|") if p.strip()]

        # Extract date if present in this row
        for part in parts:
            d = _parse_date(part)
            if d:
                current_date = d
                break

        # Extract the 3 decimal odds (always at the end)
        decimal_pat = re.compile(r"^\d+\.\d+$")
        odds_values = [p for p in parts if decimal_pat.match(p)]
        # Take the last 3 decimal numbers as h/d/a odds
        if len(odds_values) >= 2:
            avg_odds = odds_values[-3:] if len(odds_values) >= 3 else odds_values[-2:]
        else:
            avg_odds = []

        # Extract status
        status_map = {
            "finished": "final", "fin": "final",
            "after ot": "final_ot", "aot": "final_ot",
            "after so": "final_so", "aso": "final_so",
            "postponed": "postponed",
        }
        status = "unknown"
        for part in parts:
            key = part.lower()
            if key in status_map:
                status = status_map[key]
                break

        # Extract score: look for a "–" separator between numeric scores
        home_score = away_score = ""
        dash_idx = next((i for i, p in enumerate(parts) if p in ("–", "-", "−")), None)
        if dash_idx is not None and dash_idx >= 2:
            # The score before the dash
            for i in range(dash_idx - 1, max(dash_idx - 4, -1), -1):
                if parts[i].isdigit():
                    home_score = parts[i]
                    break
            # The score after the dash
            for i in range(dash_idx + 1, min(dash_idx + 4, len(parts))):
                if parts[i].isdigit():
                    away_score = parts[i]
                    break

        # Extract team names: use the link text which is the cleanest source
        link_text_parts = [p.strip() for p in h2h_link.get_text(separator="|", strip=True).split("|") if p.strip()]
        # Link text: "After OT|AOT|Eisbaren Berlin|6|6|–|5|Straubing Tigers|5"
        # or simpler: "Eisbaren Berlin|6|6|–|5|Straubing Tigers|5"
        home_team, away_team = _extract_teams(link_text_parts)

        # Emit one row per outcome using "OddsPortal Avg" as bookmaker
        outcome_types = ["home", "draw", "away"] if len(avg_odds) == 3 else ["home", "away"]
        for otype, price in zip(outcome_types, avg_odds):
            records.append({
                "source": "oddsportal",
                "league": league,
                "league_label": label,
                "game_url": game_url,
                "match_date": current_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "status": status,
                "bookmaker": "OddsPortal Avg",
                "outcome_type": otype,
                "european_odds": price,
                "fetched_at": fetched_at,
            })

    return records


def _extract_teams(parts: list[str]) -> tuple[str, str]:
    """
    From link text parts like:
      ['After OT', 'AOT', 'Eisbaren Berlin', '6', '6', '–', '5', 'Straubing Tigers', '5']
    or
      ['Eisbaren Berlin', '6', '6', '–', '5', 'Straubing Tigers', '5']
    extract home and away team names.
    """
    # Find the dash index
    dash_idx = next((i for i, p in enumerate(parts) if p in ("–", "-", "−")), None)
    if dash_idx is None:
        return "", ""

    # Home team: largest non-numeric, non-status string before dash
    status_words = {"finished", "fin", "after ot", "aot", "after so", "aso",
                    "postponed", "cancelled", "1", "x", "2"}
    home_team = ""
    for i in range(dash_idx - 1, -1, -1):
        p = parts[i]
        if not p.isdigit() and p.lower() not in status_words and len(p) > 2:
            home_team = p
            break

    away_team = ""
    for i in range(dash_idx + 1, len(parts)):
        p = parts[i]
        if not p.isdigit() and p.lower() not in status_words and len(p) > 2:
            away_team = p
            break

    return home_team, away_team


# ── Match page: per-bookmaker odds ────────────────────────────────────────────

def scrape_match_page_odds(
    page: Page,
    record: dict,
    fetched_at: str,
) -> list[dict]:
    """
    Visit individual match page and collect per-bookmaker odds.
    Returns rows with same schema as listing-page rows but one per bookmaker outcome.
    """
    url = record["game_url"]
    try:
        html = goto_wait(page, url, timeout=20000)
    except Exception as exc:
        print(f"        [!] {exc}")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Bookmaker rows: div with both h-9 and border-black-borders in class list
    bk_rows = soup.find_all(
        "div",
        class_=lambda c: c and "h-9" in c and "border-black-borders" in c
    )
    if not bk_rows:
        return []

    rows_out: list[dict] = []
    for bk_row in bk_rows:
        img = bk_row.find("img")
        bk_name = img["alt"].strip() if img and img.get("alt") else None
        if not bk_name:
            continue

        odds_divs = bk_row.find_all(attrs={"data-testid": "odd-container"})
        odds_vals: list[str] = []
        for od in odds_divs:
            nums = re.findall(r"\d+\.\d+", od.get_text())
            if nums:
                odds_vals.append(nums[0])

        outcome_types = ["home", "draw", "away"] if len(odds_vals) >= 3 else ["home", "away"]
        for otype, price in zip(outcome_types, odds_vals):
            rows_out.append({
                **record,
                "bookmaker": bk_name,
                "outcome_type": otype,
                "european_odds": price,
                "fetched_at": fetched_at,
            })

    return rows_out


# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_existing_keys(path: Path) -> set[tuple]:
    if not path.exists():
        return set()
    keys: set[tuple] = set()
    try:
        with path.open(encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                keys.add((row.get("game_url", ""), row.get("bookmaker", ""),
                           row.get("outcome_type", "")))
    except Exception:
        pass
    return keys


def append_csv(rows: list[dict], path: Path) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_existing_keys(path)
    new_rows = [
        r for r in rows
        if (r["game_url"], r["bookmaker"], r["outcome_type"]) not in existing
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


# ── Orchestration ─────────────────────────────────────────────────────────────

def scrape_league(
    page: Page,
    league: str,
    mode: str,
    num_pages: int,
    full_mode: bool,
    max_games: int | None,
    out: Path,
    fetched_at: str,
) -> int:
    cfg = LEAGUE_MAP[league]
    label = cfg["label"]
    total_written = 0

    print(f"\n  [{label}]  mode={mode}  pages={num_pages}  full={full_mode}")

    all_records: list[dict] = []
    seen_urls: set[str] = set()

    for page_num in range(1, num_pages + 1):
        slug_path = f"{cfg['country']}/{cfg['slug']}"
        if mode == "results":
            url = f"{BASE_URL}/hockey/{slug_path}/results/"
        else:
            url = f"{BASE_URL}/hockey/{slug_path}/"
        if page_num > 1:
            url += f"#/page/{page_num}/"

        print(f"    page {page_num}: {url}")
        try:
            html = goto_wait(page, url)
        except Exception as exc:
            print(f"    [!] Failed: {exc}")
            break

        records = parse_event_rows(html, league, label, fetched_at)
        # Deduplicate across pages
        new_records = [r for r in records if r["game_url"] not in seen_urls]
        for r in new_records:
            seen_urls.add(r["game_url"])
        all_records.extend(new_records)

        print(f"    Found {len(new_records)} new matches (total so far: {len(all_records)})")
        if not new_records:
            print(f"    Nothing new on page {page_num} — stopping pagination")
            break
        time.sleep(PAGE_DELAY)

    # Deduplicate game URLs for match-page visits
    unique_games: dict[str, dict] = {}
    for r in all_records:
        url = r["game_url"]
        if url not in unique_games:
            unique_games[url] = r

    if max_games is not None:
        unique_games = dict(list(unique_games.items())[:max_games])

    if not full_mode:
        # Fast mode: just write the average odds rows from the listing page
        written = append_csv(all_records, out)
        total_written += written
        print(f"    Wrote {written:,} rows (avg odds from listing pages)")
    else:
        # Full mode: visit each match page for per-bookmaker odds
        print(f"    Full mode: visiting {len(unique_games)} match pages...")
        for i, (game_url, base_record) in enumerate(unique_games.items(), start=1):
            teams = f"{base_record['home_team']} vs {base_record['away_team']}"
            print(f"      [{i:3d}/{len(unique_games)}] {base_record['match_date']} {teams}")
            bk_rows = scrape_match_page_odds(page, base_record, fetched_at)
            if bk_rows:
                written = append_csv(bk_rows, out)
                total_written += written
                print(f"        {written} new rows from {len({r['bookmaker'] for r in bk_rows})} bookmakers")
            else:
                print(f"        (no per-bookmaker data — falling back to avg odds)")
                written = append_csv(
                    [r for r in all_records if r["game_url"] == game_url],
                    out
                )
                total_written += written
            time.sleep(MATCH_DELAY)

    return total_written


def run(
    leagues: list[str],
    mode: str,
    num_pages: int,
    full_mode: bool,
    max_games: int | None,
    out: Path,
) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    label = "full (per-bookmaker)" if full_mode else "fast (avg odds)"
    print(f"\n{'─' * 60}")
    print(f"OddsPortal scraper  [{label}]  {len(leagues)} league(s)")
    print(f"Fetched at: {fetched_at}")

    total = 0
    with sync_playwright() as pw:
        browser, page = make_page(pw)

        # Accept cookie consent on first visit
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(1000)
            for sel in ["button#onetrust-accept-btn-handler",
                        "button[aria-label*='Accept']",
                        "button[class*='consent']"]:
                btn = page.query_selector(sel)
                if btn:
                    btn.click()
                    page.wait_for_timeout(800)
                    break
        except Exception:
            pass

        for league in leagues:
            written = scrape_league(
                page=page,
                league=league,
                mode=mode,
                num_pages=num_pages,
                full_mode=full_mode,
                max_games=max_games,
                out=out,
                fetched_at=fetched_at,
            )
            total += written

        browser.close()

    print(f"\nDone. Total new rows: {total:,} → {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="OddsPortal scraper — European hockey")
    parser.add_argument("--leagues", nargs="+", default=list(LEAGUE_MAP.keys()),
                        choices=list(LEAGUE_MAP.keys()), help="Leagues to scrape (default: all)")
    parser.add_argument("--mode", choices=["upcoming", "results"], default="results",
                        help="upcoming or results (default: results)")
    parser.add_argument("--pages", type=int, default=3, metavar="N",
                        help="Listing pages per league (default: 3, ~25 games each)")
    parser.add_argument("--full", action="store_true",
                        help="Visit each match page for per-bookmaker odds (slower)")
    parser.add_argument("--max-games", type=int, default=None, metavar="N",
                        help="Cap match-page visits per league in --full mode")
    parser.add_argument("--out", default=None,
                        help="Output CSV path (default: scrapers/odds/data/input/oddsportal_games.csv)")
    args = parser.parse_args()

    out = Path(args.out) if args.out else OUTPUT_FILE
    run(
        leagues=args.leagues,
        mode=args.mode,
        num_pages=args.pages,
        full_mode=args.full,
        max_games=args.max_games,
        out=out,
    )


if __name__ == "__main__":
    main()
