import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import (
    LEAGUES, load_table, num, build_sidebar,
    filter_by_season, filter_by_phase,
    PLOTLY_LAYOUT, color_map,
)

st.set_page_config(page_title="Game Results · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

games_raw = load_table("games", sel_leagues)
games     = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)

st.title("Game Results")

if games.empty:
    st.info("No game data available for the selected filters.")
    st.stop()

# ── Filter bar ────────────────────────────────────────────────────────────────
col_s, col_t = st.columns([3, 2])

with col_s:
    search = st.text_input("Search team", placeholder="e.g. Bern, München, Frölunda …")

with col_t:
    teams = sorted(set(
        games.get("home_team", pd.Series(dtype=str)).dropna().tolist() +
        games.get("away_team", pd.Series(dtype=str)).dropna().tolist()
    )) if "home_team" in games.columns else []
    sel_team = st.selectbox("Filter by team", ["All"] + teams)

filtered = games.copy()
if search:
    mask = (
        filtered.get("home_team", pd.Series(dtype=str)).str.contains(search, case=False, na=False) |
        filtered.get("away_team", pd.Series(dtype=str)).str.contains(search, case=False, na=False)
    )
    filtered = filtered[mask]
if sel_team != "All" and "home_team" in filtered.columns:
    filtered = filtered[
        (filtered["home_team"] == sel_team) |
        (filtered["away_team"] == sel_team)
    ]
if "date" in filtered.columns:
    filtered = filtered.sort_values("date", ascending=False)

# ── Results table ─────────────────────────────────────────────────────────────
cols_want = [c for c in ["date", "league_abbr", "round", "home_team",
                          "home_score", "away_score", "away_team",
                          "venue", "attendance"]
             if c in filtered.columns]
display = filtered[cols_want].rename(columns={
    "league_abbr": "league", "home_team": "home",
    "away_team": "away", "home_score": "H", "away_score": "A",
})
if "date" in display.columns:
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%d %b %Y")

st.markdown(f"**{len(display):,} games**")
st.dataframe(display, hide_index=True, use_container_width=True, height=500)

# ── Goals distribution ────────────────────────────────────────────────────────
if "home_score" in filtered.columns and "away_score" in filtered.columns:
    st.markdown("### Goals distribution")
    g = filtered.copy()
    g["total_goals"] = num(g, "home_score").add(num(g, "away_score"), fill_value=0)
    g = g.dropna(subset=["total_goals"])
    if not g.empty:
        fig = px.histogram(
            g, x="total_goals",
            color="league_abbr" if "league_abbr" in g.columns else None,
            color_discrete_map=color_map(sel_leagues),
            labels={"total_goals": "Goals in game", "count": "Games"},
            template="plotly_dark",
            nbins=20,
        )
        fig.update_layout(**PLOTLY_LAYOUT, height=280, legend_title="League")
        st.plotly_chart(fig, use_container_width=True)

# ── Home vs away win rate ─────────────────────────────────────────────────────
if "home_score" in games.columns and "away_score" in games.columns:
    st.markdown("### Home vs away win rate by league")
    rows = []
    for lg in sel_leagues:
        g = games[games["league"] == lg].copy()
        g = g.dropna(subset=["home_score", "away_score"])
        if g.empty:
            continue
        hs  = num(g, "home_score")
        aws = num(g, "away_score")
        hw  = (hs > aws).sum()
        aw  = (aws > hs).sum()
        tie = len(g) - hw - aw
        rows.append({
            "league": LEAGUES[lg]["abbr"],
            "Home win": round(hw / len(g) * 100, 1),
            "Away win": round(aw / len(g) * 100, 1),
            "Draw/OT":  round(tie / len(g) * 100, 1),
        })
    if rows:
        df_hw = pd.DataFrame(rows).melt(id_vars="league", var_name="result", value_name="pct")
        fig2 = px.bar(
            df_hw, x="league", y="pct", color="result",
            color_discrete_map={"Home win": "#4fc3f7", "Away win": "#ff6b6b", "Draw/OT": "#5a6480"},
            labels={"pct": "%", "league": ""},
            template="plotly_dark",
            barmode="stack",
        )
        fig2.update_layout(**PLOTLY_LAYOUT, height=260, legend_title="Result")
        st.plotly_chart(fig2, use_container_width=True)
