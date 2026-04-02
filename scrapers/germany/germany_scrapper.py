#!/usr/bin/env python3
"""
DEL (Deutsche Eishockey Liga) — Season Stats Scraper
=====================================================
Scrapes game data from penny-del.org (official DEL website).
The site serves static HTML — no JS rendering needed, plain requests works.

URL patterns:
  Schedule  : https://www.penny-del.org/teams/{team-slug}/spielplan
  Game main : https://www.penny-del.org/statistik/spieldetails/{DDMMYYYY}_{home}_gg_{away}_{game_id}
  Boxscore  : …/{game_slug}/boxscore
  Lineup    : …/{game_slug}/lineup
  Shots     : …/{game_slug}/shots
  Faceoffs  : …/{game_slug}/faceoffs

Season slugs used in stat pages:
  2025-26 → saison-2025-26 / hauptrunde  OR  playoff-2526
  2024-25 → saison-2024-25 / hauptrunde
  etc.

Requirements:
    pip install requests beautifulsoup4

Usage:
    python del_scraper.py --season 2025-26
    python del_scraper.py --season 2024-25 --type hauptrunde
    python del_scraper.py --season 2025-26 --type playoffs
    python del_scraper.py --season 2025-26 --type all

Outputs:
    del_games.csv      — one row per game (meta + team stats)
    del_events.csv     — one row per event (goals, penalties, GK changes)
    del_players.csv    — one row per player per game (boxscore)
    del_goalies.csv    — one row per goalkeeper per game
"""

import re
import sys
import csv
import time
import argparse
from pathlib import Path
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run:  pip install requests beautifulsoup4")
    sys.exit(1)

BASE = "https://www.penny-del.org"
DELAY = 0.4

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de,en;q=0.9",
    "Referer": "https://www.penny-del.org/",
}

# All 14 current DEL team slugs
TEAM_SLUGS = [
    "augsburger-panther",
    "eisbaeren-berlin",
    "pinguins-bremerhaven",
    "dresdner-eisloewen",
    "loewen-frankfurt",
    "erc-ingolstadt",
    "iserlohn-roosters",
    "koelner-haie",
    "adler-mannheim",
    "ehc-red-bull-muenchen",
    "nuernberg-ice-tigers",
    "schwenninger-wild-wings",
    "straubing-tigers",
    "grizzlys-wolfsburg",
]


def _to_int_or_none(value):
    """Convert scraped score fragments to int when possible."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def _norm_key(value):
    text = str(value or "").lower().strip()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def infer_scoreboard_side(team_label, game, row_index):
    """Map a scoreboard row to home/away without hardcoded team names."""
    label_key = _norm_key(team_label)
    home_candidates = {
        _norm_key(game.get("home_team")),
        _norm_key(game.get("home_slug")),
    }
    away_candidates = {
        _norm_key(game.get("away_team")),
        _norm_key(game.get("away_slug")),
    }

    if label_key and any(candidate and candidate in label_key for candidate in home_candidates):
        return "home"
    if label_key and any(candidate and candidate in label_key for candidate in away_candidates):
        return "away"

    return "home" if row_index == 0 else "away"


def finalize_game_scores(game):
    """Populate canonical total score fields used across the repo."""
    home_total = _to_int_or_none(game.get("home_score"))
    away_total = _to_int_or_none(game.get("away_score"))

    if home_total is None:
        home_total = _to_int_or_none(game.get("home_score_pT"))
    if away_total is None:
        away_total = _to_int_or_none(game.get("away_score_pT"))

    if home_total is not None:
        game["home_score"] = home_total
    if away_total is not None:
        game["away_score"] = away_total

    if home_total is not None and away_total is not None:
        game["score"] = f"{home_total}:{away_total}"

    home_ot = _to_int_or_none(game.get("home_score_pOT")) or 0
    away_ot = _to_int_or_none(game.get("away_score_pOT")) or 0
    home_so = _to_int_or_none(game.get("home_score_pSO")) or 0
    away_so = _to_int_or_none(game.get("away_score_pSO")) or 0

    game["is_overtime"] = int((home_ot + away_ot) > 0)
    game["is_shootout"] = int((home_so + away_so) > 0)

    period_scores = []
    for label in ["1", "2", "3", "OT", "SO"]:
        home_part = _to_int_or_none(game.get(f"home_score_p{label}"))
        away_part = _to_int_or_none(game.get(f"away_score_p{label}"))
        if home_part is None and away_part is None:
            continue
        period_scores.append(f"{home_part or 0}:{away_part or 0}")
    if period_scores:
        game["period_scores"] = " | ".join(period_scores)

    return game


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return BeautifulSoup(r.content, "html.parser")
        except Exception as e:
            if attempt == retries - 1:
                print(f"    [!] Failed: {url} — {e}")
                return None
            time.sleep(1.5)


# ── Discover all game URLs for a season ──────────────────────────────────────

def get_game_urls(season):
    """
    Scrapes each team's Spielplan page and collects all unique
    spieldetails URLs for the given season (e.g. '2025-26').
    Returns a sorted list of (game_slug, full_url) tuples.
    """
    # Season appears in URLs like saison-2025-26
    season_tag = f"saison-{season}"
    game_urls = {}  # slug → url (deduplicated by slug)

    print(f"  Discovering games for season {season} via team schedules …")
    for team in TEAM_SLUGS:
        url = f"{BASE}/teams/{team}/spielplan"
        soup = fetch(url)
        if not soup:
            continue

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/statistik/spieldetails/" in href:
                # Extract the slug (last path segment, no trailing slash)
                slug = href.rstrip("/").split("/statistik/spieldetails/")[-1]
                # Remove sub-pages like /boxscore
                slug = slug.split("/")[0]
                full = f"{BASE}/statistik/spieldetails/{slug}"
                if slug not in game_urls:
                    game_urls[slug] = full

        time.sleep(DELAY)

    print(f"    Found {len(game_urls)} unique games.")
    return sorted(game_urls.items())


# ── Parse main game page ──────────────────────────────────────────────────────

def parse_game_main(slug, soup, season):
    """Extract metadata and events from the main spieldetails page."""
    game = {"game_slug": slug, "season": season, "url": f"{BASE}/statistik/spieldetails/{slug}"}

    # Parse slug: DDMMYYYY_home_gg_away_NNNN
    m = re.match(r"(\d{8})_(.+?)_gg_(.+?)_(\d+)$", slug)
    if m:
        date_raw, home_slug, away_slug, game_id = m.groups()
        try:
            game["date"] = datetime.strptime(date_raw, "%d%m%Y").strftime("%Y-%m-%d")
        except ValueError:
            game["date"] = date_raw
        game["home_slug"] = home_slug
        game["away_slug"] = away_slug
        game["game_id"] = game_id

    # Game round / matchday
    round_el = soup.select_one(".game-round, .spieltag, h5")
    if round_el:
        game["round"] = round_el.get_text(strip=True)

    # Team names
    team_names = [el.get_text(strip=True) for el in soup.select("h5") if el.get_text(strip=True)]
    if len(team_names) >= 2:
        game["home_team"] = team_names[0]
        game["away_team"] = team_names[1]

    # Score from page header — look for the large score display
    score_els = soup.select(".scoreboard td, .score")
    # Parse scoreboard table: period scores
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Scoreboard" in " ".join(headers) or ("1" in headers and "T" in headers):
            rows = table.find_all("tr")
            for row_index, row in enumerate(rows[1:]):  # skip header
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 2:
                    team_label = cells[0].replace("**", "").strip()
                    scores = cells[1:]
                    side = infer_scoreboard_side(team_label, game, row_index)
                    for i, s in enumerate(scores):
                        period_label = headers[i+1] if i+1 < len(headers) else f"p{i+1}"
                        game[f"{side}_score_p{period_label}"] = s

    # Match stats table
    stats_section = soup.find(string=re.compile("Match Statistics|Spielstatistik|Schüsse auf Tor"))
    if stats_section:
        parent = stats_section.find_parent()
        if parent:
            # Walk up to find the stats container
            container = parent.find_parent(["section", "div", "article"])
            if container:
                lines = container.get_text(separator="\n").split("\n")
                lines = [l.strip() for l in lines if l.strip()]
                for i, line in enumerate(lines):
                    if "Schüsse auf Tor" in line or "Shots on Goal" in line:
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_shots_on_goal"] = vals[0]
                            game["away_shots_on_goal"] = vals[1]
                    elif "Schüsse gesamt" in line or "Total Shots" in line:
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_shots_total"] = vals[0]
                            game["away_shots_total"] = vals[1]
                    elif "Strafminuten" in line or "PIM" in line:
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_pim"] = vals[0]
                            game["away_pim"] = vals[1]
                    elif "Powerplay" in line and "quote" not in line.lower() and "tore" not in line.lower():
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_pp"] = vals[0]
                            game["away_pp"] = vals[1]
                    elif "Powerplaytore" in line:
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_pp_goals"] = vals[0]
                            game["away_pp_goals"] = vals[1]
                    elif "Bullies" in line or "Faceoffs" in line:
                        vals = [l for l in lines[i+1:i+4] if re.match(r'^\d+', l)]
                        if len(vals) >= 2:
                            game["home_faceoffs_won"] = vals[0]
                            game["away_faceoffs_won"] = vals[1]

    # Alternative: parse from structured dl/dd elements or specific class patterns
    for dl in soup.find_all(["dl", "div"]):
        txt = dl.get_text(separator="|")
        for pattern, key in [
            (r"Schüsse auf Tor\|(\d+)\|(\d+)", ("home_shots_on_goal", "away_shots_on_goal")),
            (r"Schüsse gesamt\|(\d+)\|(\d+)", ("home_shots_total", "away_shots_total")),
            (r"Strafminuten\|(\d+)\|(\d+)", ("home_pim", "away_pim")),
            (r"Powerplaytore\|(\d+)\|(\d+)", ("home_pp_goals", "away_pp_goals")),
            (r"Bullies gewonnen\|(\d+)\|(\d+)", ("home_faceoffs_won", "away_faceoffs_won")),
        ]:
            mp = re.search(pattern, txt)
            if mp and key[0] not in game:
                game[key[0]] = mp.group(1)
                game[key[1]] = mp.group(2)

    # Game info section (date, time, venue, attendance, referees)
    for li in soup.find_all("li"):
        txt = li.get_text(separator=" ").strip()
        if "Datum" in txt:
            game.setdefault("date_text", txt.replace("Datum", "").strip())
        elif "Zeit" in txt:
            game["time"] = txt.replace("Zeit", "").strip()
        elif "Ort" in txt:
            game["venue"] = txt.replace("Ort", "").strip()
        elif "Zuschauer" in txt:
            m2 = re.search(r"(\d[\d.,]+)", txt)
            if m2:
                game["attendance"] = m2.group(1).replace(".", "").replace(",", "")
        elif "Schiedsrichter" in txt:
            game.setdefault("referees", txt.replace("Schiedsrichter", "").strip())
        elif "Linesperson" in txt:
            game["linesmen"] = txt.replace("Linesperson", "").strip()

    # Parse events table
    events = []
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        # Events table has "Zeit", "Ereignis" columns
        if not any(h in headers for h in ["Zeit", "Ereignis", "Time", "Event"]):
            continue
        current_period = None
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue
            texts = [c.get_text(" ", strip=True) for c in cells]

            # Period separator rows (e.g. "1. Drittel")
            if len(texts) == 1 or (len(texts) >= 1 and re.search(r"Drittel|Overtime|Verlängerung|Penaltyschießen", texts[0])):
                current_period = texts[0]
                continue

            if len(texts) < 3:
                continue

            time_txt, team_txt, event_txt = texts[0], texts[1], " ".join(texts[2:])

            # Only process rows that look like game time stamps
            if not re.match(r"^\d+:\d+$", time_txt) and "Drittelende" not in event_txt and "Drittelstart" not in event_txt:
                continue

            event = {
                "game_slug": slug,
                "season": season,
                "period": current_period,
                "time": time_txt,
                "team": team_txt,
                "raw_event": event_txt[:300],
            }

            # Classify event type
            if re.search(r"Tor\s*\(|Tor\s+von|Tor\s+ins", event_txt):
                event["event_type"] = "goal"
                # Extract scorer
                scorer_m = re.search(r"([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)\s+Tor", event_txt)
                if scorer_m:
                    event["scorer"] = scorer_m.group(1)
                # Extract goal type (PP1, PP2, EQ, EN, etc.)
                type_m = re.search(r"Tor\s*\((\w+)\)", event_txt)
                if type_m:
                    event["goal_type"] = type_m.group(1)
                # Vorlage (assist)
                assist_m = re.search(r"Vorlage von (.+?)(?:Tor|$)", event_txt)
                if assist_m:
                    event["assists"] = assist_m.group(1).strip()
                # Score state
                score_m = re.search(r"(\d+)\s*\|\s*(\d+)", event_txt)
                if score_m:
                    event["home_score"] = score_m.group(1)
                    event["away_score"] = score_m.group(2)
                    # Determine from column positions in table
                    if len(texts) >= 4:
                        event["home_score"] = texts[-2].strip() if texts[-2].strip().isdigit() else ""
                        event["away_score"] = texts[-1].strip() if texts[-1].strip().isdigit() else ""
            elif re.search(r"Min\.\s+Strafe|Strafe gegen|Penalty", event_txt):
                event["event_type"] = "penalty"
                min_m = re.search(r"(\d+)\s+Min", event_txt)
                if min_m:
                    event["penalty_minutes"] = min_m.group(1)
                reason_m = re.search(r"wegen\s+(\S+)", event_txt)
                if reason_m:
                    event["penalty_reason"] = reason_m.group(1)
                player_m = re.search(r"gegen\s+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)", event_txt)
                if player_m:
                    event["penalized_player"] = player_m.group(1)
                    # Check if served by someone else
                    served_m = re.search(r"angetreten von\s+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)", event_txt)
                    if served_m:
                        event["served_by"] = served_m.group(1)
            elif "Torhüter" in event_txt:
                event["event_type"] = "goalkeeper_change"
                if "ins Tor" in event_txt:
                    event["gk_direction"] = "in"
                elif "aus dem Tor" in event_txt:
                    event["gk_direction"] = "out"
                gk_m = re.search(r":\s+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)", event_txt)
                if gk_m:
                    event["goalkeeper"] = gk_m.group(1)
            elif "Auszeit" in event_txt:
                event["event_type"] = "timeout"
            elif "Drittelende" in event_txt:
                event["event_type"] = "period_end"
            elif "Drittelstart" in event_txt:
                event["event_type"] = "period_start"
            else:
                event["event_type"] = "other"

            events.append(event)

    return finalize_game_scores(game), events


# ── Parse boxscore (player stats) ─────────────────────────────────────────────

def parse_boxscore(slug, soup, season):
    """Parse per-player stats from the /boxscore sub-page."""
    players = []
    goalies = []
    current_team = None
    current_role = None  # "skater" or "goalie"

    for el in soup.find_all(["h5", "h6", "table"]):
        tag = el.name

        if tag in ("h5", "h6"):
            txt = el.get_text(strip=True)
            # Team headings like "ERC Ingolstadt" or "EHC Red Bull München"
            if txt and not any(k in txt.lower() for k in ["stürmer", "verteidiger", "torhüter", "scorer", "season"]):
                current_team = txt
            if "torhüter" in txt.lower() or "goalie" in txt.lower():
                current_role = "goalie"
            elif "stürmer" in txt.lower() or "verteidiger" in txt.lower() or "skater" in txt.lower():
                current_role = "skater"
            continue

        if tag != "table":
            continue

        rows = el.find_all("tr")
        if not rows:
            continue

        # Detect header row
        header_cells = rows[0].find_all("th")
        if not header_cells:
            # Try second row
            if len(rows) > 1:
                header_cells = rows[1].find_all("th")

        headers = [th.get_text(strip=True) for th in header_cells]

        # Check what kind of table this is
        is_goalie = any(h in headers for h in ["GT", "SV%", "Saves"])
        is_skater = any(h in headers for h in ["G", "A", "PTS", "+/-", "TOI"])

        if not is_goalie and not is_skater:
            # Check section header above table
            prev = el.find_previous(["h6", "caption"])
            if prev:
                txt = prev.get_text(strip=True).lower()
                if "torhüter" in txt:
                    is_goalie = True
                else:
                    is_skater = True
            else:
                continue

        # Detect table section headers
        for row in rows:
            # Section header rows (e.g. Stürmer, Verteidiger, Torhüter)
            section_th = row.find("th", attrs={"colspan": True})
            if section_th and int(section_th.get("colspan", 1)) > 3:
                section_txt = section_th.get_text(strip=True).lower()
                if "torhüter" in section_txt:
                    current_role = "goalie"
                    is_goalie = True
                    is_skater = False
                else:
                    current_role = "skater"
                    is_skater = True
                    is_goalie = False
                continue

            cells = row.find_all("td")
            if not cells or len(cells) < 3:
                continue

            texts = [c.get_text(strip=True) for c in cells]

            # First cell should be jersey number
            try:
                jersey = int(re.sub(r"\D", "", texts[0]))
            except ValueError:
                # Might be a subheader
                if any(k in texts[0].lower() for k in ["stürmer", "verteidiger", "torhüter"]):
                    current_role = "goalie" if "torhüter" in texts[0].lower() else "skater"
                    is_goalie = current_role == "goalie"
                    is_skater = not is_goalie
                continue

            name = texts[1] if len(texts) > 1 else ""
            # Clean up name (remove captain marker, asterisk)
            name_clean = re.sub(r"\s*\(C\)|\s*\*", "", name).strip()
            is_captain = "(C)" in name
            is_starter = "*" in name

            if is_goalie or current_role == "goalie":
                g = {
                    "game_slug": slug, "season": season,
                    "team": current_team, "jersey": jersey,
                    "name": name_clean, "is_captain": is_captain, "is_starter": is_starter,
                }
                if headers:
                    col_map = {h: i for i, h in enumerate(headers)}
                    for field, col in [("goals_against", "GT"), ("saves", "Saves"),
                                       ("save_pct", "SV%"), ("minutes", "Min")]:
                        if col in col_map:
                            idx = col_map[col] + 2  # offset by jersey + name
                            if idx < len(texts):
                                g[field] = texts[idx]
                else:
                    for i, val in enumerate(texts[2:], 2):
                        g[f"col{i}"] = val
                goalies.append(g)

            else:  # skater
                p = {
                    "game_slug": slug, "season": season,
                    "team": current_team, "jersey": jersey,
                    "name": name_clean, "is_captain": is_captain, "is_starter": is_starter,
                }
                if headers:
                    col_map = {h: i for i, h in enumerate(headers)}
                    for field, col in [
                        ("goals", "G"), ("assists", "A"), ("points", "PTS"),
                        ("plus_minus", "+/-"), ("pim", "PIM"), ("shots", "SOG"),
                        ("blocks", "BLKS"), ("faceoffs_won", "FOW"), ("faceoffs_lost", "FOL"),
                        ("faceoff_pct", "FO%"), ("shifts", "Shifts"),
                        ("ice_time", "TOI"), ("pp_time", "PP TOI"), ("sh_time", "SH TOI"),
                    ]:
                        if col in col_map:
                            idx = col_map[col] + 2
                            if idx < len(texts):
                                p[field] = texts[idx]
                else:
                    for i, val in enumerate(texts[2:], 2):
                        p[f"col{i}"] = val
                players.append(p)

    return players, goalies


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        print(f"  (empty — skipping {path})")
        return
    keys = list({k for row in rows for k in row})
    priority = ["game_slug", "game_id", "season", "date", "home_team", "away_team",
                "round", "venue", "attendance", "period", "time", "event_type",
                "team", "jersey", "name"]
    cols = [c for c in priority if c in keys] + sorted(c for c in keys if c not in priority)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {len(rows):>6,} rows → {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def scrape(season, game_type="all", output_dir="."):
    print(f"\n{'─'*60}")
    print(f"Scraping DEL season {season} ({game_type}) …")

    game_urls = get_game_urls(season)

    # Optionally filter by type
    if game_type == "hauptrunde":
        game_urls = [(s, u) for s, u in game_urls if "playoff" not in s.lower()]
    elif game_type == "playoffs":
        # Playoff game IDs tend to be higher numbers; filter is tricky by slug alone
        # The slug itself doesn't clearly mark playoffs, so we keep all and rely on round field
        pass  # keep all, user can filter by 'round' column

    total = len(game_urls)
    all_games, all_events, all_players, all_goalies = [], [], [], []

    for i, (slug, url) in enumerate(game_urls, 1):
        print(f"  [{i:3d}/{total}] {slug[:55]:<55} … ", end="", flush=True)

        # Main page
        soup_main = fetch(url)
        if not soup_main:
            print("skip")
            continue
        game, events = parse_game_main(slug, soup_main, season)
        time.sleep(DELAY)

        # Boxscore
        soup_box = fetch(f"{url}/boxscore")
        players, goalies = [], []
        if soup_box:
            players, goalies = parse_boxscore(slug, soup_box, season)
        time.sleep(DELAY)

        all_games.append(game)
        all_events.extend(events)
        all_players.extend(players)
        all_goalies.extend(goalies)

        print(f"OK  ({len(events)} ev  {len(players)} pl  {len(goalies)} gk)")

    out = Path(output_dir)
    write_csv(all_games,   out / "games.csv")
    write_csv(all_events,  out / "events.csv")
    write_csv(all_players, out / "players.csv")
    write_csv(all_goalies, out / "goalies.csv")

    print(f"\nDone! Season {season}")
    print(f"  Games   : {len(all_games):,}")
    print(f"  Events  : {len(all_events):,}")
    print(f"  Players : {len(all_players):,}")
    print(f"  Goalies : {len(all_goalies):,}")


def main():
    parser = argparse.ArgumentParser(description="DEL (PENNY Deutsche Eishockey Liga) scraper")
    parser.add_argument("--season", default="2025-26",
                        help="Season to scrape e.g. 2025-26 (default: 2025-26)")
    parser.add_argument("--type", default="all", choices=["all", "hauptrunde", "playoffs"],
                        help="Game type filter (default: all)")
    parser.add_argument("--out", default=None,
                        help="Output directory for CSV files (default: scrapers/germany/data/input/)")
    args = parser.parse_args()

    out = args.out or str(Path(__file__).parent / "data" / "input")
    scrape(args.season, args.type, out)


if __name__ == "__main__":
    main()
