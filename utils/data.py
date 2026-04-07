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

            if "away_score_pT" in df.columns and "away_score" not in df.columns:
                rename_map["away_score_pT"] = "away_score"

            if "away_goals" in df.columns and "away_score" not in df.columns:
                rename_map["away_goals"] = "away_score"

            if "home_score_pT" in df.columns and "home_score" not in df.columns:
                rename_map["home_score_pT"] = "home_score"

            if "home_goals" in df.columns and "home_score" not in df.columns:
                rename_map["home_goals"] = "home_score"

            if "match_id" in df.columns and "game_id" not in df.columns:
                rename_map["match_id"] = "game_id"

            if "player" in df.columns and "name" not in df.columns:
                rename_map["player"] = "name"

            df = df.rename(columns=rename_map)

            if table == "games":
                if "score" not in df.columns and {"home_score", "away_score"}.issubset(df.columns):
                    hs = pd.to_numeric(df["home_score"], errors="coerce")
                    aws = pd.to_numeric(df["away_score"], errors="coerce")
                    df["score"] = hs.astype("Int64").astype(str) + ":" + aws.astype("Int64").astype(str)
                    df.loc[hs.isna() | aws.isna(), "score"] = pd.NA

                if "is_overtime" not in df.columns and "home_score_pOT" in df.columns and "away_score_pOT" in df.columns:
                    hot = pd.to_numeric(df["home_score_pOT"], errors="coerce").fillna(0)
                    aot = pd.to_numeric(df["away_score_pOT"], errors="coerce").fillna(0)
                    df["is_overtime"] = ((hot + aot) > 0).astype(int)

                if "is_shootout" not in df.columns and "home_score_pSO" in df.columns and "away_score_pSO" in df.columns:
                    hso = pd.to_numeric(df["home_score_pSO"], errors="coerce").fillna(0)
                    aso = pd.to_numeric(df["away_score_pSO"], errors="coerce").fillna(0)
                    df["is_shootout"] = ((hso + aso) > 0).astype(int)

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


# ── Odds data loading ─────────────────────────────────────────────────────────

ODDS_DIR = BASE_DIR / "scrapers" / "odds" / "data" / "input"


def odds_files_exist() -> bool:
    return all((ODDS_DIR / f).exists() for f in ("odds_events.csv", "odds_markets.csv", "odds_outcomes.csv"))


# ── The Odds API / OddsPortal loaders ────────────────────────────────────────

def theoddsapi_file_exists() -> bool:
    return (ODDS_DIR / "theoddsapi_games.csv").exists()


def oddsportal_file_exists() -> bool:
    return (ODDS_DIR / "oddsportal_games.csv").exists()


@st.cache_data(show_spinner=False)
def load_theoddsapi_data() -> pd.DataFrame:
    """Load theoddsapi_games.csv. Returns empty DataFrame if missing."""
    path = ODDS_DIR / "theoddsapi_games.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, low_memory=False)
        if "commence_time" in df.columns:
            df["commence_time"] = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
        if "european_odds" in df.columns:
            df["european_odds"] = pd.to_numeric(df["european_odds"], errors="coerce")
        if "point" in df.columns:
            df["point"] = pd.to_numeric(df["point"], errors="coerce")
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_oddsportal_data() -> pd.DataFrame:
    """Load oddsportal_games.csv. Returns empty DataFrame if missing."""
    path = ODDS_DIR / "oddsportal_games.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, low_memory=False)
        if "match_date" in df.columns:
            df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
        if "european_odds" in df.columns:
            df["european_odds"] = pd.to_numeric(df["european_odds"], errors="coerce")
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_odds_data() -> pd.DataFrame:
    """Load and join the three odds CSVs into a flat DataFrame.

    Returns one row per outcome (team per market per game per sportsbook).
    Columns include: betting_event_id, game_id, start_date, home_team, away_team,
    status, sportsbook, betting_market_type, betting_market_id, betting_outcome_id,
    betting_outcome_type, participant, price_american, price_decimal, result_type,
    is_available, is_suspended.
    Returns an empty DataFrame if any file is missing.
    """
    if not odds_files_exist():
        return pd.DataFrame()

    try:
        events = pd.read_csv(ODDS_DIR / "odds_events.csv", low_memory=False)
        markets = pd.read_csv(ODDS_DIR / "odds_markets.csv", low_memory=False)
        outcomes = pd.read_csv(ODDS_DIR / "odds_outcomes.csv", low_memory=False)
    except Exception:
        return pd.DataFrame()

    # Minimal event columns
    ev_cols = [c for c in ["betting_event_id", "game_id", "start_date", "home_team",
                            "away_team", "status", "season_type", "name"] if c in events.columns]
    ev = events[ev_cols].copy()

    # Minimal market columns
    mk_cols = [c for c in ["betting_event_id", "betting_market_id", "sportsbook",
                            "betting_market_type", "betting_period_type",
                            "is_main", "is_suspended"] if c in markets.columns]
    mk = markets[mk_cols].copy()

    # Minimal outcome columns
    oc_cols = [c for c in ["betting_market_id", "betting_outcome_id",
                            "betting_outcome_type", "participant",
                            "price_american", "price_decimal",
                            "result_type", "is_available"] if c in outcomes.columns]
    oc = outcomes[oc_cols].copy()

    merged = (
        ev.merge(mk, on="betting_event_id", how="inner")
          .merge(oc, on="betting_market_id", how="inner")
    )

    if "start_date" in merged.columns:
        merged["start_date"] = pd.to_datetime(merged["start_date"], errors="coerce")
    if "price_american" in merged.columns:
        merged["price_american"] = pd.to_numeric(merged["price_american"], errors="coerce")
    if "price_decimal" in merged.columns:
        merged["price_decimal"] = pd.to_numeric(merged["price_decimal"], errors="coerce")

    return merged.reset_index(drop=True)
