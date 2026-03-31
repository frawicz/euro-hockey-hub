import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import LEAGUES, available_leagues

st.set_page_config(page_title="Data & Methodology · Euro Hockey Hub", page_icon="🏒", layout="wide")

st.title("Data & Methodology")
st.caption("Where the data comes from, how it is scraped, and how metrics are computed")

st.markdown("---")

st.markdown("### Data Sources by League")

sources = {
    "austria": {
        "league": "ICE Hockey League",
        "source": "S3 JSON API (icehl.hokejovyzapis.cz)",
        "collection": "Direct API requests via Python scraper",
        "tables": "games, events, players",
    },
    "czech": {
        "league": "Czech Extraliga",
        "source": "json.esports.cz + hokej.cz game pages",
        "collection": "JSON endpoints + static HTML parsing",
        "tables": "games, events, players, goalies",
    },
    "finland": {
        "league": "Liiga",
        "source": "liiga.fi JSON API v2",
        "collection": "Direct API requests via Python scraper",
        "tables": "games, events (goal log), shotmap",
    },
    "germany": {
        "league": "DEL",
        "source": "penny-del.org",
        "collection": "Static HTML parsing",
        "tables": "games, events, players, goalies",
    },
    "slovakia": {
        "league": "Slovak Extraliga",
        "source": "hockeyslovakia.sk",
        "collection": "Static HTML + play-by-play parsing",
        "tables": "games, events, players",
    },
    "sweden": {
        "league": "SHL",
        "source": "stats.swehockey.se",
        "collection": "Static HTML parsing",
        "tables": "games, events, players, lineups",
    },
    "switzerland": {
        "league": "National League",
        "source": "Azure REST API (app-nationalleague-prod-001.azurewebsites.net)",
        "collection": "Direct API requests via Python scraper",
        "tables": "games, events, players",
    },
}

active = available_leagues()
for lg in active:
    s = sources.get(lg)
    if not s:
        continue
    st.markdown(
        f"**{LEAGUES[lg]['abbr']} · {s['league']}**  \n"
        f"Source: `{s['source']}`  \n"
        f"Collection: {s['collection']}  \n"
        f"Tables used in dashboard: `{s['tables']}`"
    )

st.markdown("---")

st.markdown("### Methodology (Brief)")
st.markdown(
    "1. **Scraping & storage**: Each league has its own scraper in `scrapers/<league>/`. Raw outputs are stored as CSV files in `scrapers/<league>/data/input/`.\n"
    "2. **Normalization layer**: Dashboard loaders standardize key columns (e.g., `home_score` / `away_score`, `game_id`, dates, league labels).\n"
    "3. **Page-level metrics**: Each page computes metrics from filtered data (league, season, phase), so numbers are always tied to the current sidebar filters.\n"
    "4. **Derived analytics**: Elo ratings, home-advantage posteriors, and goal-state transition matrices are generated from game/event sequences and saved to `data/output/` files."
)

st.markdown("---")

st.markdown("### Key Analytics Definitions")
st.markdown(
    "- **Elo**: Match-by-match rating updates based on expected vs actual result (with optional OT/SO soft handling and margin effect).\n"
    "- **Home advantage (Bayesian)**: Posterior estimate of home goal edge and home win probability using user-controlled priors.\n"
    "- **Goal-state matrix**: From each goal-difference state (e.g., -2), estimates `P(score next)` vs `P(concede next)` for teams/leagues.\n"
    "- **Power-play %**: `PP goals / PP opportunities`; computed where reliable PP opportunity denominators exist in league data."
)

st.markdown("---")

st.markdown("### Data Quality & Limitations")
st.markdown(
    "- Scraped feeds differ by league, so some fields are richer in some competitions than others.\n"
    "- Event schemas are not fully uniform (naming and granularity vary).\n"
    "- Historical corrections on source websites can change downstream values after re-scrapes.\n"
    "- Some advanced metrics depend on specific fields (e.g., PP opportunities, scoring-side tags) that may be missing in selected leagues/seasons."
)

st.markdown("---")
st.caption(
    "Transparency note: this page summarizes the current pipeline in this repository and should be updated when scrapers or metric definitions change."
)
