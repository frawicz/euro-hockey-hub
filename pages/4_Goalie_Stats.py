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

st.set_page_config(page_title="Goalie Stats · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

games_raw = load_table("games", sel_leagues)
games     = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
goalies   = filter_players_to_games(load_table("goalies", sel_leagues), games)

st.title("Goalie Stats")

if goalies.empty:
    st.info("No goalie data available for the selected filters.")
    st.stop()

# ── Controls ──────────────────────────────────────────────────────────────────
col_a, col_b = st.columns([3, 1])
with col_a:
    search_g = st.text_input("Search goalie", placeholder="Name …")
with col_b:
    min_gp = st.number_input("Min games played", min_value=1, value=5, step=1)

# ── Aggregate ─────────────────────────────────────────────────────────────────
sum_cols = [c for c in ["saves", "goals_against", "shots_against"] if c in goalies.columns]
id_cols  = [c for c in ["name", "team", "league_abbr"] if c in goalies.columns]

if id_cols:
    agg = (
        goalies.copy()
        .assign(**{c: num(goalies, c) for c in sum_cols})
        .groupby(id_cols, as_index=False)
        .agg({**{c: "sum" for c in sum_cols},
              "game_id": "count"} if "game_id" in goalies.columns
             else {c: "sum" for c in sum_cols})
    )
    if "game_id" in agg.columns:
        agg = agg.rename(columns={"game_id": "GP"})
    else:
        agg["GP"] = 1
else:
    agg = goalies.copy()
    agg["GP"] = 1

# Compute SV% and GAA
if "saves" in agg.columns:
    if "shots_against" in agg.columns:
        sa = agg["shots_against"]
    elif "goals_against" in agg.columns:
        sa = agg["saves"] + agg["goals_against"]
    else:
        sa = pd.Series(dtype=float)
    if not sa.empty:
        agg["SV%"] = (agg["saves"] / sa.replace(0, pd.NA) * 100).round(2)

if "goals_against" in agg.columns and "GP" in agg.columns:
    agg["GAA"] = (agg["goals_against"] / agg["GP"].replace(0, pd.NA)).round(2)

# ── Filters ───────────────────────────────────────────────────────────────────
if "GP" in agg.columns and min_gp > 1:
    agg = agg[agg["GP"] >= min_gp]
if search_g and "name" in agg.columns:
    agg = agg[agg["name"].str.contains(search_g, case=False, na=False)]
if "SV%" in agg.columns:
    agg = agg.sort_values("SV%", ascending=False)

show_cols = [c for c in ["name", "league_abbr", "team", "GP",
                          "saves", "goals_against", "shots_against", "SV%", "GAA"]
             if c in agg.columns]
display = agg[show_cols].rename(columns={"league_abbr": "league"})

st.markdown(f"**{len(display):,} goalies**")
st.dataframe(display, hide_index=True, use_container_width=True, height=480)

# ── SV% distribution ──────────────────────────────────────────────────────────
if "SV%" in agg.columns:
    st.markdown("### Save % distribution by league")
    fig = px.box(
        agg.dropna(subset=["SV%"]),
        x="league_abbr" if "league_abbr" in agg.columns else None,
        y="SV%",
        color="league_abbr" if "league_abbr" in agg.columns else None,
        color_discrete_map=color_map(sel_leagues),
        labels={"league_abbr": "League", "SV%": "Save %"},
        template="plotly_dark",
    )
    fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

# ── GAA vs SV% scatter ────────────────────────────────────────────────────────
if "GAA" in agg.columns and "SV%" in agg.columns:
    st.markdown("### GAA vs Save %")
    scatter = agg.dropna(subset=["GAA", "SV%"])
    if not scatter.empty:
        fig2 = px.scatter(
            scatter,
            x="GAA", y="SV%",
            color="league_abbr" if "league_abbr" in scatter.columns else None,
            color_discrete_map=color_map(sel_leagues),
            hover_name="name" if "name" in scatter.columns else None,
            hover_data=[c for c in ["team", "GP", "saves"] if c in scatter.columns],
            labels={"GAA": "Goals against avg", "SV%": "Save %"},
            template="plotly_dark",
        )
        fig2.update_traces(marker=dict(size=8, opacity=0.8, line=dict(width=0)))
        fig2.update_layout(**PLOTLY_LAYOUT, height=360, legend_title="League")
        st.plotly_chart(fig2, use_container_width=True)
