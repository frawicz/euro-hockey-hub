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

st.set_page_config(page_title="Cross-League · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

games_raw = load_table("games",   sel_leagues)
games     = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
players   = filter_players_to_games(load_table("players", sel_leagues), games)
events    = load_table("events", sel_leagues)
events    = filter_by_phase(filter_by_season(events, sel_season), sel_phase)

st.title("Cross-League Comparison")

tabs = st.tabs(["League summary", "Scoring pace", "Penalty heat", "Player nationality"])


def has_cols(df: pd.DataFrame, cols: list[str]) -> bool:
    return all(c in df.columns for c in cols)


def normalize_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "home_score" not in out.columns and "home_goals" in out.columns:
        out["home_score"] = out["home_goals"]
    if "away_score" not in out.columns and "away_goals" in out.columns:
        out["away_score"] = out["away_goals"]
    return out

# ─────────────────────────────────────────────────────────
# Tab 1 — League summary table
# ─────────────────────────────────────────────────────────
with tabs[0]:
    st.markdown("### League at a glance")
    g_all = normalize_scores(games)
    rows = []
    for lg in sel_leagues:
        g  = g_all[g_all["league"] == lg] if "league" in g_all.columns else pd.DataFrame()
        p  = players[players["league"] == lg] if "league" in players.columns else pd.DataFrame()

        hs  = num(g, "home_score")
        aws = num(g, "away_score")
        tot = hs.add(aws, fill_value=0)

        hw_pct = None
        if not g.empty and "home_score" in g.columns and "away_score" in g.columns:
            hw = (num(g, "home_score") > num(g, "away_score")).sum()
            hw_pct = round(hw / len(g) * 100, 1)

        rows.append({
            "League":       LEAGUES[lg]["label"],
            "Abbr":         LEAGUES[lg]["abbr"],
            "Games":        len(g),
            "Avg G/game":   round(tot.mean(), 2) if not tot.dropna().empty else None,
            "Home win %":   hw_pct,
            "Unique players": int(p["name"].nunique()) if "name" in p.columns else None,
        })

    if rows:
        st.dataframe(
            pd.DataFrame(rows).set_index("Abbr"),
            use_container_width=True,
        )

# ─────────────────────────────────────────────────────────
# Tab 2 — Scoring pace over the season
# ─────────────────────────────────────────────────────────
with tabs[1]:
    st.markdown("### Average goals per game — monthly trend")
    gsrc = normalize_scores(games)
    if not gsrc.empty and has_cols(gsrc, ["date", "home_score", "away_score", "league_abbr"]):
        g = gsrc.dropna(subset=["date"]).copy()
        g["date"] = pd.to_datetime(g["date"], errors="coerce")
        g = g.dropna(subset=["date"])
        if g.empty:
            st.info("No valid dates available for trend chart.")
        else:
            g["total"] = num(g, "home_score").add(num(g, "away_score"), fill_value=0)
            g["month"] = g["date"].dt.to_period("M").astype(str)
            monthly = (
                g.groupby(["month", "league_abbr"])["total"]
                .mean().round(2).reset_index()
                .rename(columns={"total": "avg_goals", "league_abbr": "league"})
            )
            if not monthly.empty:
                fig = px.line(
                    monthly, x="month", y="avg_goals", color="league",
                    color_discrete_map=color_map(sel_leagues),
                    markers=True,
                    labels={"avg_goals": "Avg goals / game", "month": "Month"},
                    template="plotly_dark",
                )
                fig.update_layout(**PLOTLY_LAYOUT, height=380, legend_title="League")
                fig.update_xaxes(gridcolor="#1e2535", tickangle=-30)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough date data to build a trend.")
    else:
        st.info("No date/score data available.")

# ─────────────────────────────────────────────────────────
# Tab 3 — Penalty heat
# ─────────────────────────────────────────────────────────
with tabs[2]:
    st.markdown("### Penalty minutes per game by league")
    if not events.empty and "event_type" in events.columns:
        penalties = events[events["event_type"].astype(str).str.lower() == "penalty"].copy()
        minute_col = next((c for c in ["minutes", "penalty_minutes", "penalty_min"] if c in penalties.columns), None)
        if minute_col and not penalties.empty:
            penalties["minutes"] = num(penalties, minute_col)
            pim = (
                penalties.groupby("league_abbr")["minutes"]
                .sum().reset_index()
                .rename(columns={"league_abbr": "league", "minutes": "total_pim"})
            )
            gpc = (
                games.groupby("league_abbr").size()
                .reset_index(name="n_games")
                .rename(columns={"league_abbr": "league"})
            )
            pim = pim.merge(gpc, on="league", how="left")
            pim["PIM/game"] = (pim["total_pim"] / pim["n_games"]).round(2)

            fig = px.bar(
                pim.sort_values("PIM/game", ascending=False),
                x="league", y="PIM/game",
                color="league",
                color_discrete_map=color_map(sel_leagues),
                labels={"PIM/game": "Penalty minutes / game", "league": ""},
                template="plotly_dark",
            )
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=320)
            st.plotly_chart(fig, use_container_width=True)

            # Penalty type breakdown
            ptype_col = next((c for c in ["penalty_type", "penalty_reason", "foul_type"] if c in penalties.columns), None)
            if ptype_col:
                st.markdown("### Most common penalty types")
                top_types = (
                    penalties.groupby(ptype_col).size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                    .head(15)
                )
                top_types = top_types.rename(columns={ptype_col: "penalty_type"})
                fig2 = px.bar(
                    top_types, x="count", y="penalty_type",
                    orientation="h",
                    labels={"count": "Count", "penalty_type": ""},
                    template="plotly_dark",
                    color_discrete_sequence=["#4fc3f7"],
                )
                fig2.update_layout(**PLOTLY_LAYOUT, height=380)
                fig2.update_yaxes(gridcolor="#1e2535", autorange="reversed")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No penalty minute data found in events.")
    else:
        st.info("No events data available.")

# ─────────────────────────────────────────────────────────
# Tab 4 — Player nationality / licence
# ─────────────────────────────────────────────────────────
with tabs[3]:
    st.markdown("### Players by nationality / licence")
    if not players.empty:
        nat_col = next(
            (c for c in ["nationality", "licence", "Licence", "nat", "country"] if c in players.columns),
            None,
        )
        if nat_col:
            if "league_abbr" not in players.columns:
                st.info("League labels are missing in player rows for this filter.")
            else:
                nat_df = (
                    players.groupby([nat_col, "league_abbr"])
                    .size().reset_index(name="count")
                    .rename(columns={nat_col: "nationality", "league_abbr": "league"})
                    .sort_values("count", ascending=False)
                    .head(40)
                )
                fig = px.bar(
                    nat_df, x="nationality", y="count", color="league",
                    color_discrete_map=color_map(sel_leagues),
                    labels={"count": "Players", "nationality": ""},
                    template="plotly_dark",
                )
                fig.update_layout(**PLOTLY_LAYOUT, height=360, legend_title="League")
                fig.update_xaxes(gridcolor="#1e2535", tickangle=-40)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(
                "No nationality/licence column found in player data. "
                "This field may not be scraped for all leagues yet."
            )
    else:
        st.info("No player data available.")
