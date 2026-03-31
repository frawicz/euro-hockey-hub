import streamlit as st
import sys
from pathlib import Path
import re
import pandas as pd
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import (
    LEAGUES,
    load_table,
    build_sidebar,
    filter_by_season,
    filter_by_phase,
    PLOTLY_LAYOUT,
    color_map,
)

st.set_page_config(page_title="Team Stats · Euro Hockey Hub", page_icon="🏒", layout="wide")


def norm_token(value: str) -> str:
    txt = str(value or "").upper().strip()
    txt = re.sub(r"[^A-Z0-9]", "", txt)
    return txt


def choose_id_col(df: pd.DataFrame) -> str | None:
    for c in ["game_id", "match_id", "game_slug"]:
        if c in df.columns:
            return c
    return None


def parse_score_pair(value: object) -> tuple[int, int] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value)
    m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m2 = re.search(r"(\d+)\s+(\d+)\s*$", s)
    if m2:
        return (int(m2.group(1)), int(m2.group(2)))
    return None


def derive_scores(games: pd.DataFrame) -> pd.DataFrame:
    out = games.copy()

    home = away = None
    for c in ["home_score", "home_goals", "home_score_pT"]:
        if c in out.columns:
            home = pd.to_numeric(out[c], errors="coerce")
            break
    for c in ["away_score", "away_goals", "away_score_pT"]:
        if c in out.columns:
            away = pd.to_numeric(out[c], errors="coerce")
            break

    if (home is None or away is None) and "score" in out.columns:
        parsed = out["score"].astype(str).str.extract(r"(\d+)\s*[:\-]\s*(\d+)")
        if home is None:
            home = pd.to_numeric(parsed[0], errors="coerce")
        if away is None:
            away = pd.to_numeric(parsed[1], errors="coerce")

    out["home_score_model"] = home if home is not None else pd.Series(dtype=float)
    out["away_score_model"] = away if away is not None else pd.Series(dtype=float)
    return out


def build_game_lookup(games: pd.DataFrame) -> tuple[str | None, dict[str, dict]]:
    id_col = choose_id_col(games)
    if id_col is None:
        return (None, {})

    lookup = {}
    for r in games.itertuples(index=False):
        gid = str(getattr(r, id_col))
        home = getattr(r, "home_team", None)
        away = getattr(r, "away_team", None)
        if home is None or away is None:
            continue

        home_tokens = {norm_token(home)}
        away_tokens = {norm_token(away)}
        for hc, ac in [("home_abbr", "away_abbr"), ("home_team_short", "away_team_short"), ("home_slug", "away_slug")]:
            hv = getattr(r, hc, None)
            av = getattr(r, ac, None)
            if hv is not None and not (isinstance(hv, float) and pd.isna(hv)):
                home_tokens.add(norm_token(hv))
            if av is not None and not (isinstance(av, float) and pd.isna(av)):
                away_tokens.add(norm_token(av))

        lookup[gid] = {
            "home_team": str(home),
            "away_team": str(away),
            "home_tokens": home_tokens,
            "away_tokens": away_tokens,
        }
    return (id_col, lookup)


def infer_side(team_value: object, game_meta: dict, team_side_value: object = None) -> str | None:
    if team_side_value is not None and not (isinstance(team_side_value, float) and pd.isna(team_side_value)):
        ts = str(team_side_value).strip().lower()
        if ts in {"home", "away"}:
            return ts
    t = norm_token(team_value)
    if t in game_meta["home_tokens"]:
        return "home"
    if t in game_meta["away_tokens"]:
        return "away"
    return None


def classify_pp_goal(league: str, row) -> bool:
    goal_type = str(getattr(row, "goal_type", "")).upper()
    goal_types = str(getattr(row, "goal_types", "")).upper()
    score_state = str(getattr(row, "score_state", "")).upper()

    if league in {"austria", "switzerland", "germany"}:
        return "PP" in goal_type
    if league == "czech":
        return goal_type in {"5/4", "5/3", "4/3"}
    if league == "slovakia":
        return goal_type.startswith("PH")
    if league == "sweden":
        return "(PP" in score_state
    if league == "finland":
        return "YV" in goal_types
    return False


def prepare_goal_events(events: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    if "event_type" in out.columns:
        out = out[out["event_type"].astype(str).str.lower().eq("goal")].copy()
    if out.empty:
        return out

    id_col = choose_id_col(out)
    if id_col is None:
        return pd.DataFrame()
    out["game_key"] = out[id_col].astype(str)

    if "event_idx" in out.columns:
        out["_ord"] = pd.to_numeric(out["event_idx"], errors="coerce")
    elif "game_time_s" in out.columns:
        out["_ord"] = pd.to_numeric(out["game_time_s"], errors="coerce")
    elif "elapsed_seconds" in out.columns:
        out["_ord"] = pd.to_numeric(out["elapsed_seconds"], errors="coerce")
    else:
        out["_ord"] = 0
    out["_ord"] = out["_ord"].fillna(0)
    out = out.sort_values(["game_key", "_ord"], kind="stable")
    return out


def compute_event_metrics(league: str, games: pd.DataFrame, events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    _, lookup = build_game_lookup(games)
    goals = prepare_goal_events(events)
    if goals.empty:
        return (pd.DataFrame(columns=["team", "pp_goals_events"]), pd.DataFrame(columns=["game_key", "first_side"]))

    pp_counter = {}
    first_goal_rows = []

    for gid, grp in goals.groupby("game_key", sort=False):
        meta = lookup.get(str(gid))
        if not meta:
            continue

        pre_h, pre_a = 0, 0
        first_side = None

        for row in grp.itertuples(index=False):
            side = None
            score_after = None

            hs = pd.to_numeric(getattr(row, "home_score", None), errors="coerce")
            aws = pd.to_numeric(getattr(row, "away_score", None), errors="coerce")
            if pd.notna(hs) and pd.notna(aws):
                score_after = (int(hs), int(aws))

            if score_after is None:
                score_after = parse_score_pair(getattr(row, "score_state", None))
            if score_after is None:
                score_after = parse_score_pair(getattr(row, "raw_event", None))

            if score_after is not None:
                ah, aa = score_after
                if ah == pre_h + 1 and aa == pre_a:
                    side = "home"
                elif aa == pre_a + 1 and ah == pre_h:
                    side = "away"
                elif ah > pre_h and aa == pre_a:
                    side = "home"
                elif aa > pre_a and ah == pre_h:
                    side = "away"

            if side is None:
                side = infer_side(getattr(row, "team", None), meta, getattr(row, "team_side", None))

            if side is None:
                continue

            if first_side is None:
                first_side = side

            team_name = meta["home_team"] if side == "home" else meta["away_team"]
            if classify_pp_goal(league, row):
                pp_counter[team_name] = pp_counter.get(team_name, 0) + 1

            if score_after is not None:
                pre_h, pre_a = score_after
            elif side == "home":
                pre_h += 1
            else:
                pre_a += 1

        if first_side is not None:
            first_goal_rows.append({"game_key": str(gid), "first_side": first_side})

    pp_df = pd.DataFrame([{"team": k, "pp_goals_events": v} for k, v in pp_counter.items()])
    fg_df = pd.DataFrame(first_goal_rows)
    return (pp_df, fg_df)


def compute_pim_per_team(games: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    if "event_type" not in events.columns:
        return pd.DataFrame(columns=["team", "pim"])

    pen = events[events["event_type"].astype(str).str.lower().eq("penalty")].copy()
    if pen.empty:
        return pd.DataFrame(columns=["team", "pim"])

    _, lookup = build_game_lookup(games)
    id_col = choose_id_col(pen)
    if id_col is None:
        return pd.DataFrame(columns=["team", "pim"])

    min_col = "penalty_minutes" if "penalty_minutes" in pen.columns else ("penalty_min" if "penalty_min" in pen.columns else None)
    if min_col is None:
        return pd.DataFrame(columns=["team", "pim"])

    pen["pim"] = pd.to_numeric(pen[min_col], errors="coerce").fillna(0)
    rows = []
    for r in pen.itertuples(index=False):
        gid = str(getattr(r, id_col))
        meta = lookup.get(gid)
        if not meta:
            continue
        side = infer_side(getattr(r, "team", None), meta, getattr(r, "team_side", None))
        if side is None:
            continue
        team_name = meta["home_team"] if side == "home" else meta["away_team"]
        rows.append({"team": team_name, "pim": float(getattr(r, "pim"))})

    if not rows:
        return pd.DataFrame(columns=["team", "pim"])
    out = pd.DataFrame(rows).groupby("team", as_index=False)["pim"].sum()
    return out


sel_leagues, sel_season, sel_phase = build_sidebar()

st.title("Team Stats")
st.caption("Team-level performance and special-teams indicators from games + events")

games_raw = load_table("games", sel_leagues)
games = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
games = derive_scores(games)
events = filter_by_phase(filter_by_season(load_table("events", sel_leagues), sel_season), sel_phase)

if games.empty:
    st.info("No game data available for selected filters.")
    st.stop()

id_col_games = choose_id_col(games)
if id_col_games is None or "home_team" not in games.columns or "away_team" not in games.columns:
    st.info("Missing required game identifiers/team columns.")
    st.stop()

valid_games = games.dropna(subset=["home_score_model", "away_score_model", "home_team", "away_team"]).copy()
if valid_games.empty:
    st.info("No valid scored games found.")
    st.stop()
valid_games["game_key"] = valid_games[id_col_games].astype(str)

# Build long team-game table.
rows = []
for r in valid_games.itertuples(index=False):
    hs = float(getattr(r, "home_score_model"))
    aws = float(getattr(r, "away_score_model"))
    home = str(getattr(r, "home_team"))
    away = str(getattr(r, "away_team"))
    lg = str(getattr(r, "league"))
    gk = str(getattr(r, "game_key"))

    def pick(*cands):
        for c in cands:
            if hasattr(r, c):
                v = getattr(r, c)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    return v
        return None

    home_ppg = pd.to_numeric(pick("home_pp_goals"), errors="coerce")
    away_ppg = pd.to_numeric(pick("away_pp_goals"), errors="coerce")
    home_ppo = pd.to_numeric(pick("home_pp_opps", "home_pp_instances"), errors="coerce")
    away_ppo = pd.to_numeric(pick("away_pp_opps", "away_pp_instances"), errors="coerce")

    rows.append(
        {
            "league": lg,
            "team": home,
            "game_key": gk,
            "is_home": 1,
            "goals_for": hs,
            "goals_against": aws,
            "is_win": 1 if hs > aws else 0,
            "is_loss": 1 if hs < aws else 0,
            "is_draw": 1 if hs == aws else 0,
            "pp_goals_game": home_ppg,
            "pp_opps_game": home_ppo,
        }
    )
    rows.append(
        {
            "league": lg,
            "team": away,
            "game_key": gk,
            "is_home": 0,
            "goals_for": aws,
            "goals_against": hs,
            "is_win": 1 if aws > hs else 0,
            "is_loss": 1 if aws < hs else 0,
            "is_draw": 1 if aws == hs else 0,
            "pp_goals_game": away_ppg,
            "pp_opps_game": away_ppo,
        }
    )

team_games = pd.DataFrame(rows)

agg = (
    team_games.groupby(["league", "team"], as_index=False)
    .agg(
        GP=("game_key", "count"),
        W=("is_win", "sum"),
        L=("is_loss", "sum"),
        D=("is_draw", "sum"),
        GF=("goals_for", "sum"),
        GA=("goals_against", "sum"),
        Home_GP=("is_home", "sum"),
        Home_W=("is_win", lambda s: int(s[team_games.loc[s.index, "is_home"] == 1].sum())),
        Away_GP=("is_home", lambda s: int((team_games.loc[s.index, "is_home"] == 0).sum())),
        Away_W=("is_win", lambda s: int(s[team_games.loc[s.index, "is_home"] == 0].sum())),
        PPG_games=("pp_goals_game", "sum"),
        PPO_games=("pp_opps_game", "sum"),
    )
)

for c in ["GP", "W", "L", "D", "GF", "GA", "Home_GP", "Home_W", "Away_GP", "Away_W", "PPG_games", "PPO_games"]:
    agg[c] = pd.to_numeric(agg[c], errors="coerce")

agg["Win%"] = (agg["W"] / agg["GP"].where(agg["GP"] != 0) * 100).round(1)
agg["GD"] = (agg["GF"] - agg["GA"]).round(0).astype(int)
agg["GF/G"] = (agg["GF"] / agg["GP"].where(agg["GP"] != 0)).round(2)
agg["GA/G"] = (agg["GA"] / agg["GP"].where(agg["GP"] != 0)).round(2)
agg["Home Win%"] = (agg["Home_W"] / agg["Home_GP"].where(agg["Home_GP"] != 0) * 100).round(1)
agg["Away Win%"] = (agg["Away_W"] / agg["Away_GP"].where(agg["Away_GP"] != 0) * 100).round(1)

# Event-derived metrics: PP goals + first goal + PIM
event_pp_parts = []
first_goal_parts = []
pim_parts = []
for lg in sel_leagues:
    g_lg = valid_games[valid_games["league"] == lg].copy()
    e_lg = events[events["league"] == lg].copy() if not events.empty and "league" in events.columns else pd.DataFrame()
    if g_lg.empty or e_lg.empty:
        continue
    pp_df, fg_df = compute_event_metrics(lg, g_lg, e_lg)
    pim_df = compute_pim_per_team(g_lg, e_lg)
    if not pp_df.empty:
        pp_df["league"] = lg
        event_pp_parts.append(pp_df)
    if not fg_df.empty:
        fg_df["league"] = lg
        first_goal_parts.append(fg_df)
    if not pim_df.empty:
        pim_df["league"] = lg
        pim_parts.append(pim_df)

if event_pp_parts:
    pp_events = pd.concat(event_pp_parts, ignore_index=True)
    agg = agg.merge(pp_events, on=["league", "team"], how="left")
else:
    agg["pp_goals_events"] = pd.NA

agg["PPG"] = agg["PPG_games"]
agg.loc[agg["PPG"].isna(), "PPG"] = agg.loc[agg["PPG"].isna(), "pp_goals_events"]
agg["PPG"] = pd.to_numeric(agg["PPG"], errors="coerce")
agg["PPO"] = pd.to_numeric(agg["PPO_games"], errors="coerce")
agg["PP%"] = (agg["PPG"] / agg["PPO"].where(agg["PPO"] != 0) * 100).round(1)

if first_goal_parts:
    fg = pd.concat(first_goal_parts, ignore_index=True)
    tg = team_games.copy()
    tg = tg.merge(fg[["league", "game_key", "first_side"]], on=["league", "game_key"], how="left")
    tg["scored_first"] = ((tg["is_home"] == 1) & (tg["first_side"] == "home")) | ((tg["is_home"] == 0) & (tg["first_side"] == "away"))
    fg_team = (
        tg.groupby(["league", "team"], as_index=False)
        .agg(
            FirstGoalScored=("scored_first", "sum"),
            FirstGoalGames=("first_side", lambda s: int(s.notna().sum())),
            FirstGoalWins=("is_win", lambda s: int(s[tg.loc[s.index, "scored_first"]].sum())),
        )
    )
    fg_team["FirstGoal Win%"] = (
        fg_team["FirstGoalWins"] / fg_team["FirstGoalScored"].where(fg_team["FirstGoalScored"] != 0) * 100
    ).round(1)
    agg = agg.merge(fg_team[["league", "team", "FirstGoalScored", "FirstGoal Win%"]], on=["league", "team"], how="left")
else:
    agg["FirstGoalScored"] = pd.NA
    agg["FirstGoal Win%"] = pd.NA

if pim_parts:
    pim_df = pd.concat(pim_parts, ignore_index=True)
    agg = agg.merge(pim_df, on=["league", "team"], how="left")
else:
    agg["pim"] = pd.NA
agg["PIM/G"] = (pd.to_numeric(agg["pim"], errors="coerce") / agg["GP"].where(agg["GP"] != 0)).round(2)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Teams", int(agg["team"].nunique()))
with col2:
    st.metric("Avg goals/team/game", f"{agg['GF/G'].mean():.2f}")
with col3:
    st.metric("Avg win rate", f"{agg['Win%'].mean():.1f}%")
with col4:
    pp_known = agg["PP%"].notna().sum()
    st.metric("Teams with PP%", int(pp_known))

# Controls
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    search = st.text_input("Search team", placeholder="Type team name...")
with c2:
    min_gp = st.number_input("Min GP", min_value=1, value=8, step=1)
with c3:
    sort_metric = st.selectbox("Sort by", ["Win%", "GF/G", "GD", "PP%", "FirstGoal Win%"])

show = agg.copy()
if search:
    show = show[show["team"].str.contains(search, case=False, na=False)]
show = show[show["GP"] >= min_gp]
show = show.sort_values(sort_metric, ascending=False, na_position="last")

st.markdown("### Team Table")
table_cols = [
    "team", "league", "GP", "W", "L", "D", "Win%", "GF", "GA", "GD", "GF/G", "GA/G",
    "Home Win%", "Away Win%", "PPG", "PPO", "PP%", "PIM/G", "FirstGoalScored", "FirstGoal Win%"
]
disp = show[table_cols].copy()
disp["league"] = disp["league"].map(lambda x: LEAGUES.get(x, {}).get("abbr", x))
disp = disp.rename(columns={"team": "Team", "league": "League"})
st.dataframe(disp, hide_index=True, use_container_width=True, height=430)

# Charts
st.markdown("### Goals Profile (GF/G vs GA/G)")
sc = show.dropna(subset=["GF/G", "GA/G"])
if not sc.empty:
    fig1 = px.scatter(
        sc,
        x="GF/G",
        y="GA/G",
        size="GP",
        color="league",
        hover_name="team",
        hover_data=["Win%", "PP%", "PIM/G"],
        color_discrete_map=color_map(sel_leagues),
        labels={"league": "League"},
        template="plotly_dark",
    )
    fig1.update_layout(**PLOTLY_LAYOUT, height=380)
    fig1.add_hline(y=sc["GA/G"].mean(), line_dash="dash", line_color="#5a6480")
    fig1.add_vline(x=sc["GF/G"].mean(), line_dash="dash", line_color="#5a6480")
    st.plotly_chart(fig1, use_container_width=True)

st.markdown("### Power-Play % (where opportunities are available)")
pp_plot = show.dropna(subset=["PP%"]).sort_values("PP%", ascending=False).head(20)
if not pp_plot.empty:
    fig2 = px.bar(
        pp_plot.sort_values("PP%", ascending=True),
        x="PP%",
        y="team",
        color="league",
        orientation="h",
        color_discrete_map=color_map(sel_leagues),
        labels={"team": "", "league": "League"},
        template="plotly_dark",
    )
    fig2.update_layout(**PLOTLY_LAYOUT, height=420)
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No official PP opportunity data in the selected leagues/filters.")

st.caption(
    "PP% is only computed where official PP opportunities exist in games data "
    "(e.g., Austria, Finland, Switzerland in your current dataset). "
    "PPG and other team metrics still use events/games where available."
)
