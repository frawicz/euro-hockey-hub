import streamlit as st
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import LEAGUES, PLOTLY_LAYOUT

st.set_page_config(page_title="Goal-State Matrix · Euro Hockey Hub", page_icon="🏒", layout="wide")

BASE_DIR = Path(__file__).parent.parent


def out_path(league: str, filename: str) -> Path:
    return BASE_DIR / "scrapers" / league / "data" / "output" / filename


@st.cache_data(show_spinner=False)
def load_team_matrix(league: str) -> pd.DataFrame:
    p = out_path(league, "goal_state_team_matrix.csv")
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    if df.empty:
        return df
    for c in ["state_diff", "n_total", "n_score", "n_concede", "p_score", "p_concede"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_league_matrix(league: str) -> pd.DataFrame:
    p = out_path(league, "goal_state_league_matrix.csv")
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    if df.empty:
        return df
    for c in ["state_diff", "n_total", "n_score", "n_concede", "p_score", "p_concede"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


available = []
for lg in LEAGUES:
    p = out_path(lg, "goal_state_team_matrix.csv")
    if p.exists():
        try:
            if pd.read_csv(p, nrows=1).shape[0] > 0:
                available.append(lg)
        except Exception:
            pass

st.title("Goal-State Transition Matrix")
st.caption("Probability to score or concede next given current goal difference")

if not available:
    st.info("No goal-state outputs found. Run `python utils/goal_state_matrix.py --league all` first.")
    st.stop()

with st.sidebar:
    st.markdown("## 🎯 Matrix Controls")
    st.markdown("---")
    labels = {LEAGUES[lg]["label"]: lg for lg in available}
    sel_label = st.selectbox("League", list(labels.keys()))
    league = labels[sel_label]

team_df = load_team_matrix(league)
league_df = load_league_matrix(league)

if team_df.empty:
    st.info("No team-level state rows for this league.")
    st.stop()

min_n = st.slider("Minimum transitions per state", min_value=1, max_value=50, value=8, step=1)
states_all = sorted(team_df["state_diff"].dropna().astype(int).unique().tolist())
if not states_all:
    st.info("No state values found.")
    st.stop()

smin, smax = int(min(states_all)), int(max(states_all))
state_range = st.slider("Goal-difference range", min_value=smin, max_value=smax, value=(max(smin, -4), min(smax, 4)))

team_opts = sorted(team_df["team"].dropna().unique().tolist())
default_teams = team_opts[:min(12, len(team_opts))]
sel_teams = st.multiselect("Teams", options=team_opts, default=default_teams)
if not sel_teams:
    st.warning("Select at least one team.")
    st.stop()

flt = team_df[
    team_df["team"].isin(sel_teams)
    & (team_df["n_total"] >= min_n)
    & (team_df["state_diff"] >= state_range[0])
    & (team_df["state_diff"] <= state_range[1])
].copy()

if flt.empty:
    st.info("No rows after filters. Lower min transitions or widen state range.")
    st.stop()

m1, m2, m3 = st.columns(3)
with m1:
    st.metric("League", LEAGUES[league]["abbr"])
with m2:
    st.metric("Teams", flt["team"].nunique())
with m3:
    st.metric("State rows", len(flt))

st.markdown("### Matrix: P(Team Scores Next | Goal Difference)")
score_pivot = (
    flt.pivot_table(index="team", columns="state_diff", values="p_score", aggfunc="mean")
    .sort_index(axis=1)
)
fig1 = px.imshow(
    score_pivot,
    aspect="auto",
    color_continuous_scale="Blues",
    zmin=0,
    zmax=1,
    labels={"x": "Goal Difference", "y": "Team", "color": "P(score next)"},
)
fig1.update_layout(**PLOTLY_LAYOUT, height=430)
st.plotly_chart(fig1, use_container_width=True)

st.markdown("### Matrix: P(Team Concedes Next | Goal Difference)")
concede_pivot = (
    flt.pivot_table(index="team", columns="state_diff", values="p_concede", aggfunc="mean")
    .sort_index(axis=1)
)
fig2 = px.imshow(
    concede_pivot,
    aspect="auto",
    color_continuous_scale="Reds",
    zmin=0,
    zmax=1,
    labels={"x": "Goal Difference", "y": "Team", "color": "P(concede next)"},
)
fig2.update_layout(**PLOTLY_LAYOUT, height=430)
st.plotly_chart(fig2, use_container_width=True)

st.markdown("### Team Detail")
focus_team = st.selectbox("Focus team", sorted(flt["team"].unique().tolist()))
t = flt[flt["team"] == focus_team].sort_values("state_diff")
if not t.empty:
    long = pd.concat(
        [
            t[["state_diff", "p_score"]].rename(columns={"p_score": "probability"}).assign(kind="Score next"),
            t[["state_diff", "p_concede"]].rename(columns={"p_concede": "probability"}).assign(kind="Concede next"),
        ],
        ignore_index=True,
    )
    fig3 = px.line(
        long,
        x="state_diff",
        y="probability",
        color="kind",
        markers=True,
        template="plotly_dark",
        labels={"state_diff": "Goal Difference", "probability": "Probability", "kind": "Type"},
        color_discrete_map={"Score next": "#4fc3f7", "Concede next": "#ff6b6b"},
    )
    fig3.update_layout(**PLOTLY_LAYOUT, height=360)
    st.plotly_chart(fig3, use_container_width=True)

st.markdown("### Filtered Data")
st.dataframe(
    flt[["team", "state_diff", "n_total", "n_score", "n_concede", "p_score", "p_concede"]]
    .rename(
        columns={
            "team": "Team",
            "state_diff": "Goal Diff",
            "n_total": "Transitions",
            "n_score": "Score Count",
            "n_concede": "Concede Count",
            "p_score": "P(Score next)",
            "p_concede": "P(Concede next)",
        }
    )
    .sort_values(["Team", "Goal Diff"]),
    hide_index=True,
    use_container_width=True,
    height=360,
)

if not league_df.empty:
    st.markdown("### League Baseline (All Teams Pooled)")
    st.dataframe(
        league_df[["state_diff", "n_total", "p_score", "p_concede"]]
        .rename(
            columns={
                "state_diff": "Goal Diff",
                "n_total": "Transitions",
                "p_score": "P(Score next)",
                "p_concede": "P(Concede next)",
            }
        )
        .sort_values("Goal Diff"),
        hide_index=True,
        use_container_width=True,
    )

st.caption(
    "Interpretation: each row models the next-goal process only. "
    "From state -2, `P(Score next)` is chance to move to -1; `P(Concede next)` is chance to move to -3."
)
