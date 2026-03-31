import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import (
    LEAGUES, load_table, num, build_sidebar,
    filter_by_season, filter_by_phase, filter_players_to_games,
    PLOTLY_LAYOUT, color_map,
)

st.set_page_config(page_title="Player Stats · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

games_raw = load_table("games", sel_leagues)
games     = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
players   = filter_players_to_games(load_table("players", sel_leagues), games)

st.title("Player Stats")

if players.empty:
    st.info("No player data available for the selected filters.")
    st.stop()

# ── Controls ──────────────────────────────────────────────────────────────────
col_a, col_b, col_c, col_d = st.columns([2, 2, 1, 1])

with col_a:
    search_p = st.text_input("Search player", placeholder="Last name …")
with col_b:
    pos_opts = sorted(players["position"].dropna().unique().tolist()) \
               if "position" in players.columns else []
    sel_pos = st.selectbox("Position", ["All"] + pos_opts)
with col_c:
    min_gp = st.number_input("Min games", min_value=1, value=5, step=1)
with col_d:
    sort_opts = [c for c in ["points", "goals", "assists", "pim", "shots", "blocks"]
                 if c in players.columns]
    sort_by = st.selectbox("Sort by", sort_opts) if sort_opts else None

# ── Aggregate per player ──────────────────────────────────────────────────────
sum_cols = [c for c in ["goals", "assists", "points", "pim", "shots", "blocks",
                         "faceoffs_won", "faceoffs_lost"]
            if c in players.columns]
id_cols  = [c for c in ["name", "team", "league_abbr", "position"] if c in players.columns]

if id_cols:
    agg = (
        players.copy()
        .assign(**{c: num(players, c) for c in sum_cols})
        .groupby(id_cols, as_index=False)
        .agg({**{c: "sum" for c in sum_cols}, "game_id": "count"} if "game_id" in players.columns
             else {c: "sum" for c in sum_cols})
    )
    if "game_id" in agg.columns:
        agg = agg.rename(columns={"game_id": "GP"})
    else:
        agg["GP"] = players.groupby(id_cols).size().values if id_cols else 1
else:
    agg = players.copy()
    agg["GP"] = 1

# Points per game
if "points" in agg.columns and "GP" in agg.columns:
    agg["P/GP"] = (agg["points"] / agg["GP"].replace(0, pd.NA)).round(2)

# ── Apply filters ─────────────────────────────────────────────────────────────
if "GP" in agg.columns and min_gp > 1:
    agg = agg[agg["GP"] >= min_gp]
if search_p and "name" in agg.columns:
    agg = agg[agg["name"].str.contains(search_p, case=False, na=False)]
if sel_pos != "All" and "position" in agg.columns:
    agg = agg[agg["position"] == sel_pos]
if sort_by and sort_by in agg.columns:
    agg = agg.sort_values(sort_by, ascending=False)

show_cols = [c for c in ["name", "league_abbr", "team", "position", "GP",
                          "goals", "assists", "points", "P/GP",
                          "pim", "shots", "blocks"]
             if c in agg.columns]
display = agg[show_cols].rename(columns={"league_abbr": "league"})

st.markdown(f"**{len(display):,} players**")
st.dataframe(display, hide_index=True, use_container_width=True, height=500)

# ── Goals vs assists scatter ──────────────────────────────────────────────────
if "goals" in agg.columns and "assists" in agg.columns:
    st.markdown("### Goals vs Assists")
    scatter_df = agg.dropna(subset=["goals", "assists"]).head(300)
    if not scatter_df.empty:
        fig = px.scatter(
            scatter_df,
            x="goals", y="assists",
            color="league_abbr" if "league_abbr" in scatter_df.columns else None,
            color_discrete_map=color_map(sel_leagues),
            hover_name="name" if "name" in scatter_df.columns else None,
            hover_data=[c for c in ["team", "GP", "points"] if c in scatter_df.columns],
            labels={"goals": "Goals", "assists": "Assists"},
            template="plotly_dark",
        )
        fig.update_traces(marker=dict(size=7, opacity=0.75, line=dict(width=0)))
        fig.update_layout(**PLOTLY_LAYOUT, height=380, legend_title="League")
        st.plotly_chart(fig, use_container_width=True)

# ── Points distribution by league ────────────────────────────────────────────
if "points" in agg.columns and "league_abbr" in agg.columns:
    st.markdown("### Points distribution by league")
    fig2 = px.box(
        agg.dropna(subset=["points"]),
        x="league_abbr", y="points",
        color="league_abbr",
        color_discrete_map=color_map(sel_leagues),
        labels={"league_abbr": "League", "points": "Points"},
        template="plotly_dark",
    )
    fig2.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=280)
    st.plotly_chart(fig2, use_container_width=True)
