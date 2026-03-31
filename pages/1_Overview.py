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

st.set_page_config(page_title="Overview · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

games_raw = load_table("games", sel_leagues)
games     = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
players = load_table("players", sel_leagues)

if not players.empty:
    if "team" in players.columns:
        players = players[players["team"].notna()].copy()

    if "goals" in players.columns:
        players["_goals"] = pd.to_numeric(players["goals"], errors="coerce")
        players = players.sort_values("_goals", ascending=False)

print("players")
print(players)
print("players")
events = load_table("events", sel_leagues)

st.title("Overview")

# ── Metrics ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.metric("Games", f"{len(games):,}")

with c2:
    if not events.empty and "event_type" in events.columns:
        n_goals = (events["event_type"] == "goal").sum()
        st.metric("Goals scored", f"{n_goals:,}")
    else:
        st.metric("Goals scored", "—")

with c3:
    if not games.empty and "home_score" in games.columns:
        total = num(games, "home_score").add(num(games, "away_score"), fill_value=0)
        avg   = total.mean()
        st.metric("Avg goals / game", f"{avg:.1f}" if pd.notna(avg) else "—")
    else:
        st.metric("Avg goals / game", "—")

with c4:
    if not players.empty and "points" in players.columns:
        top = players.assign(_pts=num(players, "points")).nlargest(1, "_pts")
        if not top.empty:
            r    = top.iloc[0]
            name = r.get("name", "—")
            lg   = r.get("league", "")
            pts  = int(r["_pts"]) if pd.notna(r["_pts"]) else 0
            st.metric("Top scorer", name, f"{pts} pts · {LEAGUES.get(lg,{}).get('abbr','')}")
        else:
            st.metric("Top scorer", "—")
    else:
        st.metric("Top scorer", "—")

with c5:
    st.metric("Leagues", len(sel_leagues))

st.markdown("---")

# ── Charts row ────────────────────────────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown("### Goals per game by league")
    if not games.empty and "home_score" in games.columns:
        rows = []
        for lg in sel_leagues:
            g   = games[games["league"] == lg]
            avg = num(g, "home_score").add(num(g, "away_score"), fill_value=0).mean()
            rows.append({"league": LEAGUES[lg]["abbr"], "gpg": round(avg, 2) if pd.notna(avg) else 0})
        if rows:
            df_gpg = pd.DataFrame(rows).sort_values("gpg", ascending=False)
            fig = px.bar(
                df_gpg, x="league", y="gpg",
                color="league",
                color_discrete_map=color_map(sel_leagues),
                labels={"gpg": "Avg goals / game", "league": ""},
                template="plotly_dark",
            )
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=280)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No score data available.")

with col_right:
    st.markdown("### Top scorers")
    if not players.empty and "points" in players.columns:
        cols_show = [c for c in ["name", "league_abbr", "team", "goals", "assists", "points"]
                     if c in players.columns]
        top10 = (
            players.assign(_pts=num(players, "points"))
            .nlargest(10, "_pts")[cols_show]
            .rename(columns={"league_abbr": "league", "goals": "G",
                              "assists": "A", "points": "PTS"})
        )
        st.dataframe(top10, hide_index=True, use_container_width=True, height=280)
    else:
        st.info("No player data available.")

# ── Recent results ────────────────────────────────────────────────────────────
st.markdown("### Recent results")
if not games.empty:
    show = games.copy()

    if "away_goals" in show.columns and "away_score" not in show.columns:
        show["away_score"] = show["away_goals"]

    if "home_goals" in show.columns and "home_score" not in show.columns:
        show["home_score"] = show["home_goals"]

    if "date" in show.columns:
        show = show.sort_values("date", ascending=False)
    cols_want = [c for c in ["date", "league_abbr", "round", "home_team",
                              "home_score", "away_score", "away_team", "venue", "attendance"]
                 if c in show.columns]
    recent = show.head(25)[cols_want].rename(columns={
        "league_abbr": "league", "home_team": "home",
        "away_team": "away", "home_score": "H", "away_score": "A",
    })
    if "date" in recent.columns:
        recent["date"] = recent["date"].dt.strftime("%d %b %Y").fillna("")
    st.dataframe(recent, hide_index=True, use_container_width=True)
else:
    st.info("No game data found. Run your scrapers first.")
