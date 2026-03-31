import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import base64
import re

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import LEAGUES, PLOTLY_LAYOUT

st.set_page_config(page_title="Elo Ratings · Euro Hockey Hub", page_icon="🏒", layout="wide")

BASE_DIR = Path(__file__).parent.parent
DEFAULT_TEAM_COLOR = "#8a94b0"


def league_color(league_key: str) -> str:
    return LEAGUES.get(league_key, {}).get("color", "#4fc3f7")


def elo_path(league_key: str, filename: str) -> Path:
    return BASE_DIR / "scrapers" / league_key / "data" / "output" / filename


def available_elo_leagues() -> list[str]:
    out = []
    for lg in LEAGUES:
        if elo_path(lg, "elo_by_round.csv").exists():
            out.append(lg)
    return out


@st.cache_data(show_spinner=False)
def load_elo_by_round(league_key: str) -> pd.DataFrame:
    p = elo_path(league_key, "elo_by_round.csv")
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    if df.empty:
        return df
    if "elo" in df.columns:
        df["elo"] = pd.to_numeric(df["elo"], errors="coerce")
    if "round_index" in df.columns:
        df["round_index"] = pd.to_numeric(df["round_index"], errors="coerce")
    if "season" in df.columns:
        df["season"] = df["season"].astype(str)
    return df


@st.cache_data(show_spinner=False)
def load_elo_game_log(league_key: str) -> pd.DataFrame:
    p = elo_path(league_key, "elo_game_log.csv")
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    if df.empty:
        return df
    if "season" in df.columns:
        df["season"] = df["season"].astype(str)
    return df


@st.cache_data(show_spinner=False)
def load_symbol_map() -> pd.DataFrame:
    p = BASE_DIR / "data" / "team_symbols_manual.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    want = [c for c in ["league", "team", "logo_path", "status"] if c in df.columns]
    if not want:
        return pd.DataFrame()
    return df[want].copy()


@st.cache_data(show_spinner=False)
def load_team_color_map() -> pd.DataFrame:
    p = BASE_DIR / "data" / "team_colors_manual.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    want = [c for c in ["league", "team", "color_hex"] if c in df.columns]
    if len(want) < 3:
        return pd.DataFrame()
    return df[want].copy()


def normalize_hex_color(value: str) -> str | None:
    if not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None
    if not v.startswith("#"):
        v = "#" + v
    if re.fullmatch(r"#[0-9a-fA-F]{6}", v):
        return v.lower()
    return None


@st.cache_data(show_spinner=False)
def logo_data_uri(rel_path: str) -> str | None:
    if not isinstance(rel_path, str) or not rel_path.strip():
        return None
    p = BASE_DIR / rel_path
    if not p.exists() or not p.is_file():
        return None
    suffix = p.suffix.lower()
    mime = "image/png"
    if suffix == ".svg":
        mime = "image/svg+xml"
    elif suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".webp":
        mime = "image/webp"
    try:
        raw = p.read_bytes()
        return f"data:{mime};base64,{base64.b64encode(raw).decode('utf-8')}"
    except Exception:
        return None


st.title("Elo Ratings")
st.caption("Team strength tracking by round within a selected league")

elo_leagues = available_elo_leagues()
if not elo_leagues:
    st.info("No Elo output found yet. Run `python utils/elo_calc.py --league all` first.")
    st.stop()

with st.sidebar:
    st.markdown("## 🏒 Elo Controls")
    st.markdown("---")
    league_labels = {LEAGUES[lg]["label"]: lg for lg in elo_leagues}
    sel_label = st.selectbox("League", list(league_labels.keys()))
    sel_league = league_labels[sel_label]

elo_round = load_elo_by_round(sel_league)
elo_games = load_elo_game_log(sel_league)
symbols = load_symbol_map()
team_colors = load_team_color_map()

if elo_round.empty:
    st.info("No Elo-by-round data available for the selected league.")
    st.stop()

season_options = sorted(elo_round["season"].dropna().unique().tolist(), reverse=True)
if not season_options:
    st.info("No season values found in Elo data.")
    st.stop()
sel_season = st.selectbox("Season", season_options, index=0)

season_df = elo_round[elo_round["season"] == sel_season].copy()
if season_df.empty:
    st.info("No Elo rows for this season.")
    st.stop()

latest_snapshot = (
    season_df.sort_values("round_index")
    .groupby("team", as_index=False)
    .tail(1)
    .sort_values("elo", ascending=False)
)

default_teams = latest_snapshot["team"].head(6).tolist()
team_options = sorted(season_df["team"].dropna().unique().tolist())
min_teams_required = 12
default_n = min(max(min_teams_required, 1), len(team_options))
default_teams = latest_snapshot["team"].head(default_n).tolist()
selected_teams = st.multiselect(
    "Teams",
    options=team_options,
    default=default_teams,
    help=f"You can select as many teams as you want (minimum {min_teams_required}).",
)

if len(selected_teams) < min_teams_required:
    st.warning(f"Select at least {min_teams_required} teams to render the table and time series.")
    st.stop()

plot_df = season_df[season_df["team"].isin(selected_teams)].copy()

# Build summary table
first_snap = (
    plot_df.sort_values("round_index")
    .groupby("team", as_index=False)
    .head(1)[["team", "elo"]]
    .rename(columns={"elo": "Start Elo"})
)
latest_snap = (
    plot_df.sort_values("round_index")
    .groupby("team", as_index=False)
    .tail(1)[["team", "round", "round_index", "elo"]]
    .rename(columns={"round": "Latest Round", "elo": "Current Elo"})
)
peak_snap = (
    plot_df.groupby("team", as_index=False)["elo"]
    .max()
    .rename(columns={"elo": "Peak Elo"})
)

summary = latest_snap.merge(first_snap, on="team", how="left").merge(peak_snap, on="team", how="left")
summary["Delta"] = (summary["Current Elo"] - summary["Start Elo"]).round(2)

if not symbols.empty:
    s = symbols[(symbols["league"] == sel_league) & (symbols["status"] == "ready")].copy()
    summary = summary.merge(s[["team", "logo_path"]], on="team", how="left")

if not elo_games.empty:
    g = elo_games[elo_games["season"] == sel_season].copy()
    gp = pd.concat(
        [
            g[["home_team"]].rename(columns={"home_team": "team"}),
            g[["away_team"]].rename(columns={"away_team": "team"}),
        ],
        ignore_index=True,
    )
    gp = gp.value_counts("team").reset_index(name="GP")
    summary = summary.merge(gp, on="team", how="left")

summary["Current Elo"] = summary["Current Elo"].round(2)
summary["Start Elo"] = summary["Start Elo"].round(2)
summary["Peak Elo"] = summary["Peak Elo"].round(2)
if "logo_path" in summary.columns:
    summary["Logo"] = summary["logo_path"].apply(logo_data_uri)

summary = summary.sort_values("Current Elo", ascending=False)
cols = [c for c in ["Logo", "team", "GP", "Current Elo", "Delta", "Peak Elo", "Latest Round", "round_index"] if c in summary.columns]
display = summary[cols].rename(columns={"team": "Team", "round_index": "Round #"})

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("League", LEAGUES[sel_league]["abbr"])
with c2:
    st.metric("Season", sel_season)
with c3:
    st.metric("Teams selected", len(selected_teams))

st.markdown("### Elo Ranking Table")
if "Logo" in display.columns:
    st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={
            "Logo": st.column_config.ImageColumn("Logo", width="small"),
        },
    )
else:
    st.dataframe(display, hide_index=True, use_container_width=True, height=320)

st.markdown("### Elo Time Series by Round")
plot_df = plot_df.sort_values(["round_index", "team"])

color_discrete_map = {team: DEFAULT_TEAM_COLOR for team in selected_teams}
if not team_colors.empty:
    tc = team_colors[team_colors["league"] == sel_league].copy()
    if not tc.empty:
        for row in tc.itertuples(index=False):
            c = normalize_hex_color(row.color_hex)
            if c and row.team in color_discrete_map:
                color_discrete_map[row.team] = c

fig = px.line(
    plot_df,
    x="round_index",
    y="elo",
    color="team",
    color_discrete_map=color_discrete_map,
    markers=True,
    hover_data={"round": True, "round_index": True, "elo": ":.2f"},
    labels={"round_index": "Round", "elo": "Elo", "team": "Team"},
    template="plotly_dark",
)
fig.update_layout(
    **PLOTLY_LAYOUT,
    height=460,
    legend_title="Team",
)
fig.update_traces(line=dict(width=2.5), marker=dict(size=6))
fig.update_xaxes(dtick=1)
fig.update_yaxes(gridcolor="#1e2535")
st.plotly_chart(fig, use_container_width=True)

st.caption(
    f"Color theme anchor for this league: {LEAGUES[sel_league]['label']} ({league_color(sel_league)})."
)
