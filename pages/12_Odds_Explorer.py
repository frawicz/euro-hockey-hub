"""
Odds Explorer — European hockey betting odds from The Odds API and OddsPortal.

Run scrapers first:
    # The Odds API (requires free API key)
    export THE_ODDS_API_KEY='your_key_here'
    python scrapers/odds/theoddsapi_scraper.py

    # OddsPortal (requires playwright)
    pip install playwright && playwright install chromium
    python scrapers/odds/oddsportal_scraper.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import (
    LEAGUES,
    PLOTLY_LAYOUT,
    build_sidebar,
    load_theoddsapi_data,
    load_oddsportal_data,
    theoddsapi_file_exists,
    oddsportal_file_exists,
)

st.set_page_config(page_title="Odds Explorer · Euro Hockey Hub", page_icon="🎲", layout="wide")
build_sidebar()

st.title("Odds Explorer")
st.caption("Pre-game moneylines, vig analysis, and cross-source comparison · The Odds API + OddsPortal")


# ── Source availability check ─────────────────────────────────────────────────

has_toa = theoddsapi_file_exists()
has_op  = oddsportal_file_exists()

if not has_toa and not has_op:
    st.warning("No odds data found from either source.")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### The Odds API")
        st.code(
            "export THE_ODDS_API_KEY='your_key'\n"
            "python scrapers/odds/theoddsapi_scraper.py",
            language="bash",
        )
        st.markdown("Get a free key at [the-odds-api.com](https://the-odds-api.com/)")
    with col2:
        st.markdown("#### OddsPortal")
        st.code(
            "pip install playwright beautifulsoup4\n"
            "playwright install chromium\n"
            "python scrapers/odds/oddsportal_scraper.py",
            language="bash",
        )
    st.stop()

toa_df = load_theoddsapi_data() if has_toa else pd.DataFrame()
op_df  = load_oddsportal_data()  if has_op  else pd.DataFrame()


# ── Helpers ───────────────────────────────────────────────────────────────────

def implied_prob(price: pd.Series) -> pd.Series:
    """Decimal odds → raw implied probability."""
    return 1.0 / price.replace(0, np.nan)


def remove_vig(home_raw: pd.Series, away_raw: pd.Series, draw_raw: pd.Series | None = None):
    """
    Normalise raw implied probs so they sum to 1.
    Returns (home_fair, away_fair, draw_fair, vig).
    """
    if draw_raw is not None:
        total = home_raw + away_raw + draw_raw
        return home_raw / total, away_raw / total, draw_raw / total, total - 1.0
    total = home_raw + away_raw
    return home_raw / total, away_raw / total, None, total - 1.0


def pivot_moneyline(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Pivot tall odds DataFrame to wide: one row per game × bookmaker.
    Input must have: outcome_type (home/draw/away), price, bookmaker,
                     home_team, away_team, date_col.
    """
    if df.empty:
        return pd.DataFrame()

    home = df[df["outcome_type"] == "home"][
        ["game_key", "bookmaker", "european_odds", "home_team", "away_team", date_col]
    ].rename(columns={"european_odds": "home_price"})

    away = df[df["outcome_type"] == "away"][
        ["game_key", "bookmaker", "european_odds"]
    ].rename(columns={"european_odds": "away_price"})

    draw = df[df["outcome_type"] == "draw"][
        ["game_key", "bookmaker", "european_odds"]
    ].rename(columns={"european_odds": "draw_price"})

    ml = home.merge(away, on=["game_key", "bookmaker"], how="inner")
    ml = ml.merge(draw, on=["game_key", "bookmaker"], how="left")

    ml["home_raw"] = implied_prob(ml["home_price"])
    ml["away_raw"] = implied_prob(ml["away_price"])
    draw_raw = implied_prob(ml["draw_price"]) if "draw_price" in ml.columns else None

    home_fair, away_fair, draw_fair, vig = remove_vig(ml["home_raw"], ml["away_raw"], draw_raw)
    ml["home_fair"] = home_fair
    ml["away_fair"] = away_fair
    if draw_fair is not None:
        ml["draw_fair"] = draw_fair
    ml["vig"] = vig

    return ml.reset_index(drop=True)


def prep_toa(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise The Odds API data for display."""
    if df.empty:
        return df
    h2h = df[df["market"] == "h2h"].copy()
    if h2h.empty:
        return pd.DataFrame()
    h2h["game_key"] = h2h["game_id"].astype(str)
    h2h["date_col"] = h2h["commence_time"]
    return h2h


def prep_op(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise OddsPortal data for display."""
    if df.empty:
        return df
    df = df.copy()
    df["game_key"] = df["game_url"].astype(str)
    df["date_col"] = df["match_date"]
    return df


# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Odds Filters")

    # Source selector
    available_sources = []
    if not toa_df.empty:
        available_sources.append("The Odds API")
    if not op_df.empty:
        available_sources.append("OddsPortal")
    sel_sources = st.multiselect("Source", available_sources, default=available_sources)

    # League selector — union of leagues in both datasets
    all_leagues: set[str] = set()
    if not toa_df.empty and "league" in toa_df.columns:
        all_leagues.update(toa_df["league"].dropna().unique())
    if not op_df.empty and "league" in op_df.columns:
        all_leagues.update(op_df["league"].dropna().unique())

    league_labels = {lg: LEAGUES[lg]["label"] for lg in all_leagues if lg in LEAGUES}
    label_to_key = {v: k for k, v in league_labels.items()}
    all_labels = sorted(league_labels.values())

    sel_labels = st.multiselect("League", all_labels, default=all_labels)
    sel_league_keys = [label_to_key[l] for l in sel_labels if l in label_to_key]

    # Date range
    min_date = None
    max_date = None
    for df_, dcol in [(toa_df, "commence_time"), (op_df, "match_date")]:
        if df_.empty or dcol not in df_.columns:
            continue
        dts = pd.to_datetime(df_[dcol], errors="coerce").dropna()
        if dts.empty:
            continue
        d_min = dts.min().date()
        d_max = dts.max().date()
        min_date = d_min if min_date is None else min(min_date, d_min)
        max_date = d_max if max_date is None else max(max_date, d_max)

    if min_date and max_date:
        sel_dates = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
            date_from, date_to = sel_dates
        else:
            date_from, date_to = min_date, max_date
    else:
        date_from, date_to = None, None

    # Bookmaker filter
    all_books: set[str] = set()
    if not toa_df.empty and "bookmaker" in toa_df.columns:
        all_books.update(toa_df["bookmaker"].dropna().unique())
    if not op_df.empty and "bookmaker" in op_df.columns:
        all_books.update(op_df["bookmaker"].dropna().unique())
    all_books_list = sorted(all_books)
    sel_books = st.multiselect(
        "Bookmakers",
        all_books_list,
        default=all_books_list[:10] if len(all_books_list) > 10 else all_books_list,
    )


# ── Filter helpers ────────────────────────────────────────────────────────────

def apply_filters(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty:
        return df
    if sel_league_keys and "league" in df.columns:
        df = df[df["league"].isin(sel_league_keys)]
    if sel_books and "bookmaker" in df.columns:
        df = df[df["bookmaker"].isin(sel_books)]
    if date_from and date_to and date_col in df.columns:
        dts = pd.to_datetime(df[date_col], errors="coerce")
        df = df[dts.dt.date.between(date_from, date_to)]
    return df


toa_filtered = apply_filters(prep_toa(toa_df), "date_col") if "The Odds API" in sel_sources else pd.DataFrame()
op_filtered  = apply_filters(prep_op(op_df),   "date_col") if "OddsPortal"   in sel_sources else pd.DataFrame()


# ── Data availability status bar ──────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("The Odds API games", f"{toa_filtered['game_key'].nunique():,}" if not toa_filtered.empty and "game_key" in toa_filtered.columns else "–")
with c2:
    st.metric("OddsPortal games", f"{op_filtered['game_key'].nunique():,}" if not op_filtered.empty and "game_key" in op_filtered.columns else "–")
with c3:
    n_books_toa = toa_filtered["bookmaker"].nunique() if not toa_filtered.empty and "bookmaker" in toa_filtered.columns else 0
    n_books_op  = op_filtered["bookmaker"].nunique()  if not op_filtered.empty  and "bookmaker" in op_filtered.columns  else 0
    st.metric("Total bookmakers", n_books_toa + n_books_op)
with c4:
    leagues_covered = set()
    for df_ in [toa_filtered, op_filtered]:
        if not df_.empty and "league" in df_.columns:
            leagues_covered.update(df_["league"].dropna().unique())
    st.metric("Leagues covered", len(leagues_covered))


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_games, tab_vig, tab_compare, tab_raw = st.tabs([
    "Upcoming Games", "Vig Analysis", "Cross-Source Comparison", "Raw Data"
])


# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 — Upcoming Games
# ──────────────────────────────────────────────────────────────────────────────
with tab_games:
    st.markdown("### Upcoming Games with Best Available Odds")
    st.caption("Best (highest) decimal odds per outcome across all selected bookmakers.")

    frames = []
    for df_, date_col, source_label in [
        (toa_filtered, "commence_time", "The Odds API"),
        (op_filtered,  "match_date",    "OddsPortal"),
    ]:
        if df_.empty:
            continue
        upcoming = df_[df_["status"].isin(["upcoming", ""]) | df_["status"].isna()].copy() if "status" in df_.columns else df_.copy()
        if upcoming.empty:
            continue
        upcoming["_source"] = source_label
        upcoming["_date"] = pd.to_datetime(upcoming.get("date_col", upcoming.get(date_col)), errors="coerce")
        frames.append(upcoming)

    if not frames:
        st.info("No upcoming games in the selected filters.")
    else:
        combined = pd.concat(frames, ignore_index=True)

        # Best odds per game × outcome_type across all bookmakers
        if {"game_key", "outcome_type", "european_odds", "home_team", "away_team"}.issubset(combined.columns):
            best = (
                combined.groupby(["league_label", "game_key", "home_team", "away_team", "_date", "outcome_type"])["european_odds"]
                .max()
                .reset_index()
                .pivot_table(
                    index=["league_label", "game_key", "home_team", "away_team", "_date"],
                    columns="outcome_type",
                    values="european_odds",
                    aggfunc="max",
                )
                .reset_index()
            )
            best.columns.name = None

            # Compute vig-removed implied probs
            if "home" in best.columns and "away" in best.columns:
                best["home_raw"] = implied_prob(best["home"])
                best["away_raw"] = implied_prob(best["away"])
                draw_raw = implied_prob(best["draw"]) if "draw" in best.columns else None
                hf, af, df_fair, vig = remove_vig(best["home_raw"], best["away_raw"], draw_raw)
                best["home_prob%"] = (hf * 100).round(1)
                best["away_prob%"] = (af * 100).round(1)
                if df_fair is not None:
                    best["draw_prob%"] = (df_fair * 100).round(1)
                best["vig%"] = (vig * 100).round(2)

            best = best.sort_values("_date").rename(columns={
                "_date": "Date",
                "home_team": "Home",
                "away_team": "Away",
                "league_label": "League",
                "home": "Best Home",
                "away": "Best Away",
                "draw": "Best Draw",
            })
            display_cols = [c for c in ["Date", "League", "Home", "Away",
                                         "Best Home", "Best Draw", "Best Away",
                                         "home_prob%", "draw_prob%", "away_prob%", "vig%"]
                            if c in best.columns]
            if "Date" in best.columns:
                best["Date"] = pd.to_datetime(best["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(best[display_cols], hide_index=True, use_container_width=True)
        else:
            st.info("Upcoming games data is missing required columns (home_team, away_team, outcome_type, european_odds).")


# ──────────────────────────────────────────────────────────────────────────────
# TAB 2 — Vig Analysis
# ──────────────────────────────────────────────────────────────────────────────
with tab_vig:
    st.markdown("### Vig (Overround) by Bookmaker and Source")
    st.caption("Overround = sum of raw implied probs − 1. Lower = better value for bettors.")

    vig_frames = []

    for df_, date_col, source_label in [
        (toa_filtered, "commence_time", "The Odds API"),
        (op_filtered,  "match_date",    "OddsPortal"),
    ]:
        if df_.empty:
            continue
        ml = pivot_moneyline(df_, "date_col")
        if ml.empty:
            continue
        ml["source"] = source_label
        vig_frames.append(ml)

    if not vig_frames:
        st.info("Not enough data to compute vig. Need at least home and away odds per game.")
    else:
        vig_combined = pd.concat(vig_frames, ignore_index=True)

        vig_summary = (
            vig_combined.groupby(["source", "bookmaker"])["vig"]
            .agg(avg="mean", median="median", n="count")
            .reset_index()
            .sort_values("avg")
        )
        vig_summary["avg"] *= 100
        vig_summary["median"] *= 100

        fig_vig = px.bar(
            vig_summary,
            x="avg",
            y="bookmaker",
            color="source",
            orientation="h",
            text=vig_summary["avg"].round(2).astype(str) + "%",
            template="plotly_dark",
            labels={"avg": "Avg vig (%)", "bookmaker": "Bookmaker", "source": "Source"},
            color_discrete_map={"The Odds API": "#4fc3f7", "OddsPortal": "#ffb703"},
        )
        fig_vig.update_layout(
            **{**PLOTLY_LAYOUT,
               "xaxis": dict(title="Average vig (%)", gridcolor="#1e2535"),
               "yaxis": dict(title="")},
            height=max(250, len(vig_summary) * 28 + 60),
            showlegend=True,
        )
        fig_vig.update_coloraxes(showscale=False)
        st.plotly_chart(fig_vig, use_container_width=True)

        st.dataframe(
            vig_summary.rename(columns={
                "source": "Source", "bookmaker": "Bookmaker",
                "avg": "Avg vig %", "median": "Median vig %", "n": "Games"
            }).round(3),
            hide_index=True,
            use_container_width=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
# TAB 3 — Cross-Source Comparison
# ──────────────────────────────────────────────────────────────────────────────
with tab_compare:
    st.markdown("### Cross-Source Odds Comparison")
    st.caption("Compare home win implied probability for the same games across The Odds API and OddsPortal.")

    if toa_filtered.empty or op_filtered.empty:
        st.info("Both sources must have data to compare. Load data from both scrapers first.")
    else:
        # Build per-game average fair home prob for each source
        def source_avg_home_prob(df_: pd.DataFrame) -> pd.DataFrame:
            ml = pivot_moneyline(df_, "date_col")
            if ml.empty:
                return pd.DataFrame()
            return (
                ml.groupby(["game_key", "home_team", "away_team"])["home_fair"]
                .mean()
                .reset_index()
            )

        toa_probs = source_avg_home_prob(toa_filtered)
        op_probs  = source_avg_home_prob(op_filtered)

        if toa_probs.empty or op_probs.empty:
            st.info("Could not compute fair probabilities from one or both sources.")
        else:
            # Fuzzy join on home_team + away_team (exact match for now)
            merged = toa_probs.merge(
                op_probs,
                on=["home_team", "away_team"],
                how="inner",
                suffixes=("_toa", "_op"),
            )

            if merged.empty:
                st.info(
                    "No games matched between sources by team names. "
                    "This may be due to different team name formats between The Odds API and OddsPortal."
                )
            else:
                merged["diff_pp"] = ((merged["home_fair_toa"] - merged["home_fair_op"]) * 100).round(2)
                merged["label"] = merged["home_team"] + " vs " + merged["away_team"]

                fig_cmp = go.Figure()
                fig_cmp.add_trace(go.Scatter(
                    x=merged["home_fair_toa"] * 100,
                    y=merged["home_fair_op"] * 100,
                    mode="markers",
                    marker=dict(size=8, color="#4fc3f7", opacity=0.7),
                    text=merged["label"],
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "The Odds API: %{x:.1f}%<br>"
                        "OddsPortal: %{y:.1f}%<extra></extra>"
                    ),
                ))
                # Perfect agreement line
                lim = [0, 100]
                fig_cmp.add_trace(go.Scatter(
                    x=lim, y=lim, mode="lines",
                    line=dict(color="#3a4460", dash="dash", width=1),
                    name="Perfect agreement",
                ))
                fig_cmp.update_layout(
                    **{**PLOTLY_LAYOUT,
                       "xaxis": dict(title="Home win % · The Odds API", gridcolor="#1e2535"),
                       "yaxis": dict(title="Home win % · OddsPortal", gridcolor="#1e2535")},
                    height=420,
                )
                st.plotly_chart(fig_cmp, use_container_width=True)

                with st.expander("Comparison table"):
                    st.dataframe(
                        merged[["label", "home_fair_toa", "home_fair_op", "diff_pp"]].rename(columns={
                            "label": "Game",
                            "home_fair_toa": "Home prob (The Odds API)",
                            "home_fair_op": "Home prob (OddsPortal)",
                            "diff_pp": "Diff (pp)",
                        }).round(3),
                        hide_index=True,
                        use_container_width=True,
                    )


# ──────────────────────────────────────────────────────────────────────────────
# TAB 4 — Raw Data
# ──────────────────────────────────────────────────────────────────────────────
with tab_raw:
    st.markdown("### Raw Odds Data")

    subtab_toa, subtab_op = st.tabs(["The Odds API", "OddsPortal"])

    with subtab_toa:
        if toa_filtered.empty:
            st.info(
                "No data from The Odds API yet.\n\n"
                "```bash\nexport THE_ODDS_API_KEY='your_key'\n"
                "python scrapers/odds/theoddsapi_scraper.py\n```"
            )
        else:
            display_cols = [c for c in [
                "commence_time", "league_label", "home_team", "away_team",
                "bookmaker", "market", "outcome_type", "outcome_name",
                "european_odds", "point", "status",
            ] if c in toa_filtered.columns]
            disp = toa_filtered[display_cols].copy()
            if "commence_time" in disp.columns:
                disp["commence_time"] = disp["commence_time"].dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(disp, hide_index=True, use_container_width=True)

    with subtab_op:
        if op_filtered.empty:
            st.info(
                "No data from OddsPortal yet.\n\n"
                "```bash\npip install playwright beautifulsoup4\n"
                "playwright install chromium\n"
                "python scrapers/odds/oddsportal_scraper.py\n```"
            )
        else:
            display_cols = [c for c in [
                "match_date", "match_time", "league_label",
                "home_team", "away_team",
                "home_score", "away_score",
                "bookmaker", "outcome_type", "european_odds",
            ] if c in op_filtered.columns]
            disp = op_filtered[display_cols].copy()
            if "match_date" in disp.columns:
                disp["match_date"] = disp["match_date"].dt.strftime("%Y-%m-%d")
            st.dataframe(disp, hide_index=True, use_container_width=True)
