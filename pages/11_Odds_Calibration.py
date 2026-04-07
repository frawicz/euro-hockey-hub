"""
Odds Calibration — compare sportsbook-implied probabilities against actual game results.

Loads data from scrapers/odds/data/input/{odds_events,odds_markets,odds_outcomes}.csv.
Run the scraper first:
    export SPORTSDATAIO_KEY="your_key"
    python scrapers/odds/sportsdataio_odds.py --sport nhl --season 2026
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
    PLOTLY_LAYOUT,
    build_sidebar,
    load_odds_data,
    odds_files_exist,
)

st.set_page_config(page_title="Odds Calibration · Euro Hockey Hub", page_icon="📊", layout="wide")

# Sidebar (league/season filters aren't meaningful here, but keep nav consistent)
build_sidebar()

st.title("Odds Calibration")
st.caption("Market-implied probabilities vs. actual results · vig analysis · sportsbook comparison")

# ── Guard: no data yet ────────────────────────────────────────────────────────
if not odds_files_exist():
    st.warning("No odds data found.")
    st.markdown(
        "Run the odds scraper to populate the data:\n\n"
        "```bash\n"
        "export SPORTSDATAIO_KEY='your_key_here'\n\n"
        "# Full NHL season\n"
        "python scrapers/odds/sportsdataio_odds.py --sport nhl --season 2026\n\n"
        "# Single date (quick test)\n"
        "python scrapers/odds/sportsdataio_odds.py --sport nhl --date 2026-04-02 --limit-events 5\n"
        "```\n\n"
        "Outputs to `scrapers/odds/data/input/`: `odds_events.csv`, `odds_markets.csv`, `odds_outcomes.csv`"
    )
    st.stop()

raw = load_odds_data()
if raw.empty:
    st.error("Odds files exist but could not be loaded. Check the CSV format.")
    st.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────

def american_to_prob(american: pd.Series) -> pd.Series:
    """Convert American odds to raw implied probability (includes vig)."""
    p = pd.Series(np.nan, index=american.index, dtype=float)
    pos = american > 0
    neg = american < 0
    p[pos] = 100.0 / (american[pos] + 100.0)
    p[neg] = american[neg].abs() / (american[neg].abs() + 100.0)
    return p


def remove_vig_two_way(home_raw: pd.Series, away_raw: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Normalise raw implied probs so home + away = 1. Returns (home_fair, away_fair, vig)."""
    total = home_raw + away_raw
    home_fair = home_raw / total
    away_fair = away_raw / total
    vig = total - 1.0
    return home_fair, away_fair, vig


# ── Sidebar filters for this page ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Odds Filters")

    all_books = sorted(raw["sportsbook"].dropna().unique().tolist()) if "sportsbook" in raw.columns else []
    sel_books = st.multiselect("Sportsbooks", all_books, default=all_books[:5] if len(all_books) >= 5 else all_books)

    all_market_types = sorted(raw["betting_market_type"].dropna().unique().tolist()) if "betting_market_type" in raw.columns else []
    sel_market = st.selectbox("Market type", all_market_types if all_market_types else ["Moneyline"], index=0)

    if "betting_period_type" in raw.columns:
        all_periods = sorted(raw["betting_period_type"].dropna().unique().tolist())
        sel_period = st.selectbox("Period", all_periods, index=0)
    else:
        sel_period = None


# ── Filter to selected market + period + sportsbooks ─────────────────────────
mask = pd.Series(True, index=raw.index)
if sel_books:
    mask &= raw["sportsbook"].isin(sel_books)
if "betting_market_type" in raw.columns:
    mask &= raw["betting_market_type"] == sel_market
if sel_period and "betting_period_type" in raw.columns:
    mask &= raw["betting_period_type"] == sel_period

df = raw[mask].copy()
if df.empty:
    st.info("No data matches the current filters.")
    st.stop()

# ── Build moneyline pivot (one game × sportsbook row, wide format) ─────────────
# Identify home and away outcome types — handles "HomeTeamMoneyline" / "AwayTeamMoneyline"
# and looser labels
def _is_home_outcome(ot: str) -> bool:
    ot_lower = str(ot).lower()
    return "home" in ot_lower and "away" not in ot_lower

def _is_away_outcome(ot: str) -> bool:
    ot_lower = str(ot).lower()
    return "away" in ot_lower

if "betting_outcome_type" not in df.columns:
    st.error("Expected column 'betting_outcome_type' not found in odds data.")
    st.stop()

df["_is_home"] = df["betting_outcome_type"].apply(_is_home_outcome)
df["_is_away"] = df["betting_outcome_type"].apply(_is_away_outcome)
df["_implied_raw"] = american_to_prob(df["price_american"])

home_df = df[df["_is_home"]].copy()
away_df = df[df["_is_away"]].copy()

join_cols = [c for c in ["betting_event_id", "game_id", "start_date", "home_team", "away_team", "status", "sportsbook"] if c in df.columns]

ml = home_df[join_cols + ["price_american", "_implied_raw", "result_type"]].merge(
    away_df[["betting_event_id", "sportsbook", "price_american", "_implied_raw", "result_type"]],
    on=["betting_event_id", "sportsbook"],
    suffixes=("_home", "_away"),
    how="inner",
)

if ml.empty:
    st.info("Could not build a moneyline home/away pairing from the current data. Check that 'HomeTeamMoneyline' and 'AwayTeamMoneyline' outcome types are present.")
    st.stop()

ml["home_fair"], ml["away_fair"], ml["vig"] = remove_vig_two_way(ml["_implied_raw_home"], ml["_implied_raw_away"])

# Actual result from home team's perspective
if "result_type_home" in ml.columns:
    ml["home_result"] = ml["result_type_home"].str.strip().str.lower().map({"win": 1, "loss": 0, "push": np.nan})
else:
    ml["home_result"] = np.nan

n_total = ml["betting_event_id"].nunique()
n_final = ml[ml["home_result"].notna()]["betting_event_id"].nunique()
n_books = ml["sportsbook"].nunique() if "sportsbook" in ml.columns else 0
avg_vig = ml["vig"].mean()

# ── Top metrics ───────────────────────────────────────────────────────────────
st.markdown("### Summary")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Games in dataset", f"{n_total:,}")
with c2:
    st.metric("Games with results", f"{n_final:,}")
with c3:
    st.metric("Sportsbooks", n_books)
with c4:
    st.metric("Avg vig (overround)", f"{avg_vig * 100:.2f}%")

# ── Calibration curve ─────────────────────────────────────────────────────────
st.markdown("### Calibration Curve")
st.caption(
    "For each sportsbook, games are grouped by their vig-removed implied home win probability. "
    "Each point shows the mean predicted probability vs the actual home win rate in that bucket. "
    "A well-calibrated market sits on the diagonal."
)

calibrated = ml[ml["home_result"].notna()].copy()
if calibrated.empty:
    st.info("No completed games with known results found — calibration requires `result_type` to be populated in the outcomes CSV.")
else:
    calibrated["prob_bucket"] = (calibrated["home_fair"] * 20).round(0) / 20  # 5% bins

    cal_grouped = (
        calibrated.groupby(["sportsbook", "prob_bucket"])
        .agg(
            mean_implied=("home_fair", "mean"),
            actual_rate=("home_result", "mean"),
            n=("home_result", "count"),
        )
        .reset_index()
    )
    cal_grouped = cal_grouped[cal_grouped["n"] >= 3]  # require at least 3 games per cell

    if cal_grouped.empty:
        st.info("Not enough completed games per probability bucket to draw a calibration curve (need ≥ 3 per 5% bin per sportsbook).")
    else:
        fig_cal = go.Figure()
        # Perfect calibration line
        fig_cal.add_trace(
            go.Scatter(
                x=[0, 1], y=[0, 1],
                mode="lines",
                line=dict(color="#3a4460", dash="dash", width=1),
                name="Perfect calibration",
                showlegend=True,
            )
        )
        colors = px.colors.qualitative.Plotly
        for i, book in enumerate(sorted(cal_grouped["sportsbook"].unique())):
            sub = cal_grouped[cal_grouped["sportsbook"] == book].sort_values("mean_implied")
            fig_cal.add_trace(
                go.Scatter(
                    x=sub["mean_implied"],
                    y=sub["actual_rate"],
                    mode="markers+lines",
                    name=book,
                    marker=dict(size=sub["n"].clip(upper=30) * 0.6 + 4, color=colors[i % len(colors)]),
                    line=dict(color=colors[i % len(colors)], width=1.5),
                    customdata=sub[["n", "prob_bucket"]].values,
                    hovertemplate=(
                        "<b>%{fullData.name}</b><br>"
                        "Implied: %{x:.1%}<br>"
                        "Actual win rate: %{y:.1%}<br>"
                        "Games: %{customdata[0]}<extra></extra>"
                    ),
                )
            )
        fig_cal.update_layout(
            **PLOTLY_LAYOUT,
            height=440,
            xaxis=dict(title="Implied probability (vig-removed)", tickformat=".0%", gridcolor="#1e2535"),
            yaxis=dict(title="Actual home win rate", tickformat=".0%", gridcolor="#1e2535"),
        )
        st.plotly_chart(fig_cal, use_container_width=True)

        with st.expander("Calibration data table"):
            st.dataframe(
                cal_grouped.rename(columns={
                    "sportsbook": "Sportsbook",
                    "prob_bucket": "Prob bucket",
                    "mean_implied": "Mean implied",
                    "actual_rate": "Actual win rate",
                    "n": "Games",
                }).round(3),
                hide_index=True,
                use_container_width=True,
            )

# ── Vig distribution ──────────────────────────────────────────────────────────
st.markdown("### Vig Distribution by Sportsbook")
st.caption("Overround = sum of raw implied probabilities − 1. Higher = more juice taken by the book.")

vig_per_game = ml.groupby(["sportsbook", "betting_event_id"])["vig"].mean().reset_index()
vig_summary = (
    vig_per_game.groupby("sportsbook")["vig"]
    .agg(["mean", "median", "min", "max", "count"])
    .reset_index()
    .sort_values("mean")
    .rename(columns={"sportsbook": "Sportsbook", "mean": "Avg vig", "median": "Median vig",
                     "min": "Min vig", "max": "Max vig", "count": "Games"})
)
vig_summary[["Avg vig", "Median vig", "Min vig", "Max vig"]] *= 100

fig_vig = px.bar(
    vig_summary.sort_values("Avg vig"),
    x="Avg vig",
    y="Sportsbook",
    orientation="h",
    text=vig_summary.sort_values("Avg vig")["Avg vig"].round(2).astype(str) + "%",
    template="plotly_dark",
    labels={"Avg vig": "Average vig (%)"},
    color="Avg vig",
    color_continuous_scale="Blues",
)
fig_vig.update_layout(**PLOTLY_LAYOUT, height=max(200, len(vig_summary) * 35 + 60), showlegend=False)
fig_vig.update_coloraxes(showscale=False)
st.plotly_chart(fig_vig, use_container_width=True)

st.dataframe(vig_summary.round(3), hide_index=True, use_container_width=True)

# ── Sportsbook odds comparison for individual games ───────────────────────────
st.markdown("### Sportsbook Comparison")
st.caption("Select a game to compare home win implied probability across all sportsbooks.")

game_labels = {}
if {"home_team", "away_team", "start_date", "betting_event_id"}.issubset(ml.columns):
    for eid, grp in ml.groupby("betting_event_id"):
        row = grp.iloc[0]
        date_str = pd.to_datetime(row["start_date"]).strftime("%Y-%m-%d") if pd.notna(row.get("start_date")) else "?"
        game_labels[eid] = f"{date_str}  {row['home_team']} vs {row['away_team']}"

if game_labels:
    sel_event = st.selectbox("Game", list(game_labels.values()), index=0, key="odds_game_sel")
    sel_eid = [k for k, v in game_labels.items() if v == sel_event]
    if sel_eid:
        game_df = ml[ml["betting_event_id"] == sel_eid[0]].sort_values("sportsbook").copy()

        fig_game = go.Figure()
        fig_game.add_trace(
            go.Bar(
                x=game_df["sportsbook"],
                y=game_df["home_fair"] * 100,
                name="Home win %",
                marker_color="#4fc3f7",
                text=(game_df["home_fair"] * 100).round(1).astype(str) + "%",
                textposition="outside",
            )
        )
        fig_game.add_trace(
            go.Bar(
                x=game_df["sportsbook"],
                y=game_df["away_fair"] * 100,
                name="Away win %",
                marker_color="#ffb703",
                text=(game_df["away_fair"] * 100).round(1).astype(str) + "%",
                textposition="outside",
            )
        )
        first_row = game_df.iloc[0]
        title_text = f"{first_row.get('home_team', 'Home')} vs {first_row.get('away_team', 'Away')}"
        fig_game.update_layout(
            **PLOTLY_LAYOUT,
            barmode="group",
            height=340,
            title=dict(text=title_text, font=dict(size=13, color="#8a94b0")),
            xaxis=dict(title="", gridcolor="#1e2535"),
            yaxis=dict(title="Implied probability (%)", gridcolor="#1e2535"),
        )
        st.plotly_chart(fig_game, use_container_width=True)

        # Raw odds table for this game
        display_game = game_df[["sportsbook", "price_american_home", "price_american_away",
                                 "home_fair", "away_fair", "vig"]].copy()
        display_game["home_fair"] *= 100
        display_game["away_fair"] *= 100
        display_game["vig"] *= 100
        st.dataframe(
            display_game.rename(columns={
                "sportsbook": "Sportsbook",
                "price_american_home": "Home odds (American)",
                "price_american_away": "Away odds (American)",
                "home_fair": "Home implied %",
                "away_fair": "Away implied %",
                "vig": "Vig %",
            }).round(2),
            hide_index=True,
            use_container_width=True,
        )

# ── Raw odds table ────────────────────────────────────────────────────────────
with st.expander("Full moneyline table", expanded=False):
    show_cols = [c for c in ["start_date", "home_team", "away_team", "sportsbook",
                              "price_american_home", "price_american_away",
                              "home_fair", "away_fair", "vig",
                              "home_result", "status"] if c in ml.columns]
    disp = ml[show_cols].copy()
    for pct_col in ["home_fair", "away_fair", "vig"]:
        if pct_col in disp.columns:
            disp[pct_col] = (disp[pct_col] * 100).round(2)
    if "start_date" in disp.columns:
        disp["start_date"] = disp["start_date"].dt.strftime("%Y-%m-%d")
    st.dataframe(
        disp.rename(columns={
            "start_date": "Date",
            "home_team": "Home",
            "away_team": "Away",
            "sportsbook": "Book",
            "price_american_home": "Home odds",
            "price_american_away": "Away odds",
            "home_fair": "Home implied %",
            "away_fair": "Away implied %",
            "vig": "Vig %",
            "home_result": "Home W/L",
            "status": "Status",
        }),
        hide_index=True,
        use_container_width=True,
    )
