"""
utils/data.py — shared data loading and league config for all dashboard pages.
"""

import streamlit as st
import pandas as pd
from pathlib import Path

# ── League registry ───────────────────────────────────────────────────────────

LEAGUES = {
    "austria":     {"label": "ICE Hockey League",  "abbr": "ICE", "color": "#c77dff"},
    "czech":       {"label": "Czech Extraliga",     "abbr": "CZE", "color": "#d62828"},
    "finland":     {"label": "Liiga",               "abbr": "FIN", "color": "#57cc6b"},
    "germany":     {"label": "DEL",                 "abbr": "DEL", "color": "#ffd166"},
    "sweden":      {"label": "SHL",                 "abbr": "SHL", "color": "#4fc3f7"},
    "switzerland": {"label": "National League",     "abbr": "NL",  "color": "#ff6b6b"},
    "slovakia":    {"label": "Slovak Extraliga",    "abbr": "SVK", "color": "#9999ff"},
    "khl":         {"label": "KHL",                 "abbr": "KHL", "color": "#e63946"},
}

TABLES = ["games", "events", "players", "goalies", "shotmap"]

BASE_DIR = Path(__file__).parent.parent  # repo root


# ── CSV discovery ─────────────────────────────────────────────────────────────

def csv_path(league: str, table: str) -> Path | None:
    """Return path to scrapers/{league}/data/input/{table}.csv if it exists."""
    p = BASE_DIR / "scrapers" / league / "data" / "input" / f"{table}.csv"
    return p if p.exists() else None


def available_leagues() -> list[str]:
    """Leagues that have at least a games.csv."""
    return [lg for lg in LEAGUES if csv_path(lg, "games") is not None]


# ── Loading ───────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_table(table: str, leagues: list[str]) -> pd.DataFrame:
    frames = []

    for lg in leagues:
        path = csv_path(lg, table)
        if path is None:
            continue

        try:
            df = pd.read_csv(path, low_memory=False)

            rename_map = {}

            if "away_goals" in df.columns and "away_score" not in df.columns:
                rename_map["away_goals"] = "away_score"

            if "home_goals" in df.columns and "home_score" not in df.columns:
                rename_map["home_goals"] = "home_score"

            if "match_id" in df.columns and "game_id" not in df.columns:
                rename_map["match_id"] = "game_id"

            if "player" in df.columns and "name" not in df.columns:
                rename_map["player"] = "name"

            df = df.rename(columns=rename_map)

            df["league"] = lg
            df["league_abbr"] = LEAGUES[lg]["abbr"]
            df["league_label"] = LEAGUES[lg]["label"]

            frames.append(df)

        except Exception:
            pass

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    for col in ("date", "Date", "game_date", "gameDate"):
        if col in out.columns:
            out["date"] = pd.to_datetime(out[col], errors="coerce")
            break

    return out


def num(df: pd.DataFrame, col: str) -> pd.Series:
    """Coerce column to numeric; return empty Series if column absent."""
    if col not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


# ── Filters ───────────────────────────────────────────────────────────────────

def filter_by_season(df: pd.DataFrame, season: str) -> pd.DataFrame:
    if season == "All seasons" or "season" not in df.columns:
        return df
    return df[df["season"].astype(str) == str(season)]


def filter_by_phase(df: pd.DataFrame, phase: str) -> pd.DataFrame:
    if phase == "All":
        return df
    for col in ("status", "round"):
        if col in df.columns:
            return df[df[col].astype(str) == phase]
    return df


def filter_players_to_games(players: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    return players


# ── Sidebar builder ───────────────────────────────────────────────────────────

def build_sidebar(games_raw: pd.DataFrame | None = None):
    """
    Render shared sidebar filters. Returns (sel_leagues, sel_season, sel_phase).
    Call this at the top of every page.
    """
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] ul > li:first-child a {
            text-transform: capitalize !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("## 🏒 Euro Hockey Hub")
        st.markdown("---")

        active = available_leagues()
        if not active:
            st.error("No data found. Run your scrapers first.")
            st.stop()

        league_options = ["All leagues"] + [LEAGUES[lg]["label"] for lg in active]
        sel_label = st.selectbox("League", league_options)
        sel_leagues = (
            active if sel_label == "All leagues"
            else [lg for lg in active if LEAGUES[lg]["label"] == sel_label]
        )

        # Load games if not passed in (used for season/phase options)
        if games_raw is None or games_raw.empty:
            games_raw = load_table("games", sel_leagues)

        seasons = (
            sorted(games_raw["season"].dropna().unique().tolist(), reverse=True)
            if "season" in games_raw.columns else []
        )
        sel_season = st.selectbox("Season", ["All seasons"] + seasons) if seasons else "All seasons"

        phase_col = next((c for c in ("status", "round") if c in games_raw.columns), None)
        if phase_col:
            phases = sorted(games_raw[phase_col].dropna().unique().tolist())
            sel_phase = st.selectbox("Phase", ["All"] + phases)
        else:
            sel_phase = "All"

        st.markdown("---")
        st.markdown(
            f"<small style='color:#3a4460'>{len(sel_leagues)} league(s) loaded</small>",
            unsafe_allow_html=True,
        )

    return sel_leagues, sel_season, sel_phase


# ── Shared Plotly theme ───────────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(15,22,36,1)",
    margin=dict(l=0, r=0, t=10, b=0),
    font=dict(family="DM Mono, monospace", size=11, color="#8a94b0"),
    xaxis=dict(gridcolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535"),
    legend_title_font_color="#8a94b0",
)

def color_map(leagues: list[str]) -> dict:
    return {LEAGUES[lg]["abbr"]: LEAGUES[lg]["color"] for lg in leagues}
