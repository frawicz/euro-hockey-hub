"""Scrape club symbols (logos) for hockey teams and save local assets + mapping CSVs.

Usage:
    python utils/scrape_club_symbols.py --league all
    python utils/scrape_club_symbols.py --league czech

Outputs:
    assets/club_symbols/<league>/<team_slug>.<ext>
    scrapers/<league>/data/output/team_symbols.csv
    data/team_symbols_all_leagues.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent

LEAGUES = {
    "austria": "ICE Hockey League",
    "czech": "Czech Extraliga",
    "finland": "Liiga",
    "germany": "DEL",
    "slovakia": "Slovak Extraliga",
    "sweden": "SHL",
    "switzerland": "National League",
}

WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
UA = "EuroHockeyHub-SymbolScraper/1.0 (https://github.com/)"


def norm_text(value: str) -> str:
    txt = str(value or "").lower().strip()
    txt = txt.replace("&", " and ")
    txt = re.sub(r"[^\w\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def slugify(value: str) -> str:
    txt = str(value or "").strip().lower()
    txt = re.sub(r"[^a-z0-9]+", "-", txt)
    txt = re.sub(r"-+", "-", txt).strip("-")
    return txt or "team"


def canonical_search_name(team: str) -> str:
    txt = str(team or "").strip()
    txt = re.sub(r"\b(a\.s\.|s\.r\.o\.|spol\.\s*s\s*r\.o\.)\b", " ", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\b(hokejov[ýy]\s+klub|mestsk[ýy]\s+hokejov[ýy]\s+klub)\b", " ", txt, flags=re.IGNORECASE)
    txt = txt.replace("’", "'")
    txt = re.sub(r"[(),]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def extension_from_url(url: str) -> str:
    path = unquote(urlparse(url).path).lower()
    if path.endswith(".svg"):
        return "svg"
    if path.endswith(".jpg") or path.endswith(".jpeg"):
        return "jpg"
    if path.endswith(".webp"):
        return "webp"
    return "png"


def load_teams_for_league(league: str) -> list[str]:
    p = BASE_DIR / "scrapers" / league / "data" / "input" / "games.csv"
    if not p.exists():
        return []

    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception:
        return []

    if "home_team" not in df.columns or "away_team" not in df.columns:
        return []

    teams = set(df["home_team"].dropna().astype(str).str.strip().tolist())
    teams.update(df["away_team"].dropna().astype(str).str.strip().tolist())
    teams = sorted(t for t in teams if t)
    return teams


def wiki_apis_for_league(league: str) -> list[str]:
    # Native language wiki first, then English fallback.
    if league == "czech":
        return ["https://cs.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "germany":
        return ["https://de.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "finland":
        return ["https://fi.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "sweden":
        return ["https://sv.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "austria":
        return ["https://de.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "switzerland":
        return ["https://de.wikipedia.org/w/api.php", "https://fr.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    if league == "slovakia":
        return ["https://sk.wikipedia.org/w/api.php", "https://cs.wikipedia.org/w/api.php", "https://en.wikipedia.org/w/api.php"]
    return ["https://en.wikipedia.org/w/api.php"]


def wiki_search_candidates(session: requests.Session, wiki_api: str, team: str, league_label: str) -> list[str]:
    team_clean = canonical_search_name(team)
    queries = [
        f'"{team}" {league_label} ice hockey',
        f'"{team_clean}" {league_label} ice hockey',
        f'"{team}" ice hockey',
        f'"{team_clean}" ice hockey',
        f'"{team}" hockey',
        f'"{team_clean}" hockey',
        team,
        team_clean,
    ]

    titles: list[str] = []
    for q in queries:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": q,
            "srlimit": 6,
            "format": "json",
        }
        try:
            r = session.get(wiki_api, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue
        for row in data.get("query", {}).get("search", []):
            title = row.get("title")
            if title and title not in titles:
                titles.append(title)
    return titles[:12]


def title_score(team: str, title: str) -> float:
    a = norm_text(team)
    b = norm_text(title)
    if not a or not b:
        return 0.0
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    inter = len(a_tokens & b_tokens)
    base = inter / max(1, len(a_tokens))
    # Bonus when full team name appears as contiguous text.
    if a in b:
        base += 0.35
    # Penalize likely non-team pages.
    bad_words = {"season", "list", "draft", "playoff", "league", "cup", "championship"}
    if b_tokens & bad_words:
        base -= 0.20
    return max(0.0, min(1.0, base))


def best_wiki_title(team: str, titles: list[str]) -> tuple[str | None, float]:
    if not titles:
        return (None, 0.0)
    scored = sorted(((t, title_score(team, t)) for t in titles), key=lambda x: x[1], reverse=True)
    best_title, best_score = scored[0]
    if best_score < 0.30:
        return (None, best_score)
    return (best_title, best_score)


def title_to_wikidata_qid(session: requests.Session, wiki_api: str, title: str) -> str | None:
    params = {
        "action": "query",
        "prop": "pageprops",
        "titles": title,
        "format": "json",
    }
    try:
        r = session.get(wiki_api, params=params, timeout=20)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
    except Exception:
        return None
    for page in pages.values():
        qid = page.get("pageprops", {}).get("wikibase_item")
        if qid:
            return qid
    return None


def title_to_page_image_url(session: requests.Session, wiki_api: str, title: str) -> str | None:
    params = {
        "action": "query",
        "prop": "pageimages",
        "titles": title,
        "piprop": "original",
        "format": "json",
    }
    try:
        r = session.get(wiki_api, params=params, timeout=20)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
    except Exception:
        return None
    for page in pages.values():
        original = page.get("original", {})
        src = original.get("source")
        if src:
            return src
    return None


def wikidata_logo_filename(session: requests.Session, qid: str) -> str | None:
    try:
        r = session.get(WIKIDATA_ENTITY.format(qid=qid), timeout=20)
        r.raise_for_status()
        entity = r.json().get("entities", {}).get(qid, {})
    except Exception:
        return None
    claims = entity.get("claims", {})

    # Prefer official logo (P154), fallback to image (P18).
    for prop in ("P154", "P18"):
        arr = claims.get(prop, [])
        for claim in arr:
            mainsnak = claim.get("mainsnak", {})
            dv = mainsnak.get("datavalue", {})
            value = dv.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def commons_file_url(session: requests.Session, filename: str) -> str | None:
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "url",
        "format": "json",
    }
    try:
        r = session.get(COMMONS_API, params=params, timeout=20)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
    except Exception:
        return None
    for page in pages.values():
        infos = page.get("imageinfo", [])
        if infos and infos[0].get("url"):
            return infos[0]["url"]
    return None


def download_file(session: requests.Session, url: str, out_path: Path) -> bool:
    try:
        r = session.get(url, timeout=40)
        r.raise_for_status()
    except Exception:
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.content)
    return True


def scrape_team_symbol(
    session: requests.Session,
    league: str,
    team: str,
    delay_s: float,
    debug: bool = False,
) -> dict:
    league_label = LEAGUES[league]

    result = {
        "league": league,
        "league_label": league_label,
        "team": team,
        "wiki_title": None,
        "wiki_api": None,
        "wikidata_qid": None,
        "logo_filename": None,
        "logo_url": None,
        "local_path": None,
        "status": "not_found",
        "score": 0.0,
    }

    try:
        team_match_name = canonical_search_name(team)
        best_title = None
        best_score = 0.0
        best_api = None
        for wiki_api in wiki_apis_for_league(league):
            titles = wiki_search_candidates(session, wiki_api, team, league_label)
            title, score = best_wiki_title(team_match_name, titles)
            if debug:
                print(f"    debug api={wiki_api} candidates={len(titles)} best={title} score={score:.3f}")
            if title and score > best_score:
                best_title = title
                best_score = score
                best_api = wiki_api

        result["score"] = round(best_score, 3)
        if not best_title or not best_api:
            result["status"] = "no_good_title"
            return result

        result["wiki_title"] = best_title
        result["wiki_api"] = best_api
        qid = title_to_wikidata_qid(session, best_api, best_title)
        logo_url = None

        if qid:
            result["wikidata_qid"] = qid
            logo_filename = wikidata_logo_filename(session, qid)
            if logo_filename:
                result["logo_filename"] = logo_filename
                logo_url = commons_file_url(session, logo_filename)

        # Fallback: page image from selected wiki page when no logo URL exists.
        if not logo_url:
            logo_url = title_to_page_image_url(session, best_api, best_title)
            if logo_url:
                result["logo_filename"] = None

        if not logo_url:
            result["status"] = "no_logo_url"
            return result

        result["logo_url"] = logo_url
        ext = extension_from_url(logo_url)
        out_rel = Path("assets") / "club_symbols" / league / f"{slugify(team)}.{ext}"
        out_abs = BASE_DIR / out_rel
        ok = download_file(session, logo_url, out_abs)
        if not ok:
            result["status"] = "download_failed"
            return result

        result["local_path"] = out_rel.as_posix()
        result["status"] = "ok"
        return result
    finally:
        if delay_s > 0:
            time.sleep(delay_s)


def save_outputs(rows: list[dict], leagues: list[str]) -> None:
    out_all = BASE_DIR / "data"
    out_all.mkdir(parents=True, exist_ok=True)
    all_path = out_all / "team_symbols_all_leagues.csv"
    new_df = pd.DataFrame(rows)

    if all_path.exists():
        old_df = pd.read_csv(all_path, low_memory=False)
        # Replace rows for targeted leagues, preserve others.
        old_keep = old_df[~old_df["league"].isin(leagues)].copy() if "league" in old_df.columns else old_df
        all_df = pd.concat([old_keep, new_df], ignore_index=True)
    else:
        all_df = new_df

    all_df = all_df.sort_values(["league", "team"], kind="stable").reset_index(drop=True)
    all_df.to_csv(all_path, index=False, quoting=csv.QUOTE_MINIMAL)

    for lg in leagues:
        part = all_df[all_df["league"] == lg].copy()
        out_lg = BASE_DIR / "scrapers" / lg / "data" / "output"
        out_lg.mkdir(parents=True, exist_ok=True)
        part.to_csv(out_lg / "team_symbols.csv", index=False, quoting=csv.QUOTE_MINIMAL)


def parse_args():
    p = argparse.ArgumentParser(description="Scrape club symbols/logos for leagues in this repo")
    p.add_argument("--league", nargs="+", default=["all"], help="league key(s) or all")
    p.add_argument("--delay", type=float, default=0.15, help="delay between teams in seconds")
    p.add_argument("--debug-team", default=None, help="optional team name to debug matching on")
    return p.parse_args()


def main():
    args = parse_args()
    req = [x.lower() for x in args.league]

    if "all" in req:
        targets = list(LEAGUES.keys())
    else:
        bad = [x for x in req if x not in LEAGUES]
        if bad:
            raise SystemExit(f"Unknown league(s): {', '.join(bad)}")
        targets = req

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    rows: list[dict] = []
    for lg in targets:
        teams = load_teams_for_league(lg)
        if not teams:
            print(f"[{lg}] no teams found in games.csv")
            continue

        print(f"[{lg}] scraping symbols for {len(teams)} teams...")
        for idx, team in enumerate(teams, start=1):
            if args.debug_team and team != args.debug_team:
                continue
            row = scrape_team_symbol(
                session,
                lg,
                team,
                args.delay,
                debug=bool(args.debug_team),
            )
            rows.append(row)
            print(f"  {idx:>2}/{len(teams)} {team} -> {row['status']}")

    if not rows:
        print("No rows scraped.")
        return

    save_outputs(rows, targets)

    df = pd.DataFrame(rows)
    ok = (df["status"] == "ok").sum()
    print(f"Done. {ok}/{len(df)} teams with saved symbol files.")


if __name__ == "__main__":
    main()
