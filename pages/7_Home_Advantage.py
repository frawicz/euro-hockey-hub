import streamlit as st
import sys
from pathlib import Path
import numpy as np
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

st.set_page_config(page_title="Home Advantage · Euro Hockey Hub", page_icon="🏒", layout="wide")


def derive_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

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

    if home is None:
        home = pd.Series(dtype=float)
    if away is None:
        away = pd.Series(dtype=float)

    out["home_score_model"] = home
    out["away_score_model"] = away
    return out


def bayes_goal_edge(diff: pd.Series, mu0: float, sigma0: float, sigma_lik_override: float | None):
    x = pd.to_numeric(diff, errors="coerce").dropna()
    n = len(x)
    if n == 0:
        return None

    xbar = float(x.mean())
    sample_std = float(x.std(ddof=1)) if n > 1 else 1.0
    sigma_lik = sigma_lik_override if sigma_lik_override is not None else max(sample_std, 0.8)
    sigma_lik = max(float(sigma_lik), 0.05)

    prior_var = sigma0 ** 2
    lik_var = sigma_lik ** 2

    post_var = 1.0 / ((1.0 / prior_var) + (n / lik_var))
    post_mean = post_var * ((mu0 / prior_var) + (n * xbar / lik_var))
    post_sd = float(np.sqrt(post_var))

    return {
        "n": n,
        "xbar": xbar,
        "sigma_lik": sigma_lik,
        "post_mean": float(post_mean),
        "post_sd": post_sd,
        "ci_low": float(post_mean - 1.96 * post_sd),
        "ci_high": float(post_mean + 1.96 * post_sd),
    }


def bayes_home_win_rate(home_scores: pd.Series, away_scores: pd.Series, alpha0: float, beta0: float):
    hs = pd.to_numeric(home_scores, errors="coerce")
    aws = pd.to_numeric(away_scores, errors="coerce")
    mask = hs.notna() & aws.notna()
    hs = hs[mask]
    aws = aws[mask]
    if len(hs) == 0:
        return None

    decisive = hs != aws
    w = int((hs > aws).sum())
    l = int((aws > hs).sum())

    alpha_post = alpha0 + w
    beta_post = beta0 + l

    # Monte Carlo CI to avoid extra dependency.
    samples = np.random.beta(alpha_post, beta_post, size=6000)

    return {
        "decisive_games": int(decisive.sum()),
        "home_wins": w,
        "away_wins": l,
        "draws": int((~decisive).sum()),
        "post_mean": float(alpha_post / (alpha_post + beta_post)),
        "ci_low": float(np.quantile(samples, 0.025)),
        "ci_high": float(np.quantile(samples, 0.975)),
    }


sel_leagues, sel_season, sel_phase = build_sidebar()

st.title("Home Advantage")
st.caption("Estimate home-ice edge with user-controlled Bayesian priors")

games_raw = load_table("games", sel_leagues)
games = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)
games = derive_scores(games)

if games.empty:
    st.info("No game data available for selected filters.")
    st.stop()

with st.expander("Bayesian Prior Controls", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        mu0 = st.slider("Prior mean: home goal edge", min_value=-1.5, max_value=1.5, value=0.15, step=0.05)
        sigma0 = st.slider("Prior SD: home goal edge", min_value=0.10, max_value=2.50, value=0.60, step=0.05)
    with c2:
        alpha0 = st.slider("Beta prior alpha (home win rate)", min_value=0.5, max_value=20.0, value=2.0, step=0.5)
        beta0 = st.slider("Beta prior beta (home win rate)", min_value=0.5, max_value=20.0, value=2.0, step=0.5)
    with c3:
        manual_sigma = st.checkbox("Use fixed likelihood SD", value=False)
        sigma_lik_override = st.slider("Fixed SD for goal edge likelihood", min_value=0.20, max_value=4.00, value=1.40, step=0.05) if manual_sigma else None

def build_posterior_row(group_df: pd.DataFrame, label: str, league_key: str, team_name: str | None = None):
    hs = group_df["home_score_model"]
    aws = group_df["away_score_model"]
    valid = hs.notna() & aws.notna()
    g = group_df[valid]
    if g.empty:
        return None

    diff = g["home_score_model"] - g["away_score_model"]
    b_goal = bayes_goal_edge(diff, mu0=mu0, sigma0=sigma0, sigma_lik_override=sigma_lik_override)
    b_win = bayes_home_win_rate(g["home_score_model"], g["away_score_model"], alpha0=alpha0, beta0=beta0)
    if b_goal is None or b_win is None:
        return None

    return {
        "label": label,
        "team": team_name,
        "league_key": league_key,
        "games": b_goal["n"],
        "raw_goal_edge": round(b_goal["xbar"], 3),
        "goal_edge_post": round(b_goal["post_mean"], 3),
        "goal_edge_low": round(b_goal["ci_low"], 3),
        "goal_edge_high": round(b_goal["ci_high"], 3),
        "raw_home_win_rate": round((b_win["home_wins"] / max(b_win["home_wins"] + b_win["away_wins"], 1)), 3),
        "home_win_post": round(b_win["post_mean"], 3),
        "home_win_low": round(b_win["ci_low"], 3),
        "home_win_high": round(b_win["ci_high"], 3),
        "home_wins": b_win["home_wins"],
        "away_wins": b_win["away_wins"],
        "draws": b_win["draws"],
    }


tab_league, tab_team = st.tabs(["By League", "By Team"])

with tab_league:
    league_rows = []
    for lg in sel_leagues:
        g = games[games["league"] == lg].copy()
        row = build_posterior_row(g, label=LEAGUES[lg]["abbr"], league_key=lg)
        if row:
            league_rows.append(row)

    if not league_rows:
        st.info("No valid score data to estimate home advantage by league.")
        st.stop()

    res = pd.DataFrame(league_rows).sort_values("goal_edge_post", ascending=False)

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Leagues modeled", len(res))
    with m2:
        st.metric("Avg posterior home goal edge", f"{res['goal_edge_post'].mean():.2f}")
    with m3:
        st.metric("Avg posterior home win prob", f"{(res['home_win_post'].mean()*100):.1f}%")

    st.markdown("### League Comparison Table")
    table_cols = [
        "label", "games", "goal_edge_post", "goal_edge_low", "goal_edge_high",
        "home_win_post", "home_win_low", "home_win_high", "home_wins", "away_wins", "draws"
    ]
    st.dataframe(
        res[table_cols].rename(
            columns={
                "label": "League",
                "games": "Games",
                "goal_edge_post": "Post Goal Edge",
                "goal_edge_low": "Goal Edge CI Low",
                "goal_edge_high": "Goal Edge CI High",
                "home_win_post": "Post Home Win Prob",
                "home_win_low": "Win Prob CI Low",
                "home_win_high": "Win Prob CI High",
                "home_wins": "Home W",
                "away_wins": "Away W",
                "draws": "Draw/OT",
            }
        ),
        hide_index=True,
        use_container_width=True,
        height=320,
    )

    st.markdown("### Posterior Home Goal Edge (with 95% CI)")
    fig1 = px.scatter(
        res,
        x="label",
        y="goal_edge_post",
        error_y=(res["goal_edge_high"] - res["goal_edge_post"]),
        error_y_minus=(res["goal_edge_post"] - res["goal_edge_low"]),
        color="label",
        color_discrete_map=color_map([x for x in res["league_key"].tolist()]),
        template="plotly_dark",
        labels={"label": "League", "goal_edge_post": "Home goal edge"},
    )
    fig1.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=360)
    fig1.add_hline(y=0, line_dash="dash", line_color="#5a6480")
    st.plotly_chart(fig1, use_container_width=True)

    st.markdown("### Posterior Home Win Probability (with 95% CI)")
    fig2 = px.scatter(
        res,
        x="label",
        y="home_win_post",
        error_y=(res["home_win_high"] - res["home_win_post"]),
        error_y_minus=(res["home_win_post"] - res["home_win_low"]),
        color="label",
        color_discrete_map=color_map([x for x in res["league_key"].tolist()]),
        template="plotly_dark",
        labels={"label": "League", "home_win_post": "Home win probability"},
    )
    fig2.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=360)
    fig2.add_hline(y=0.5, line_dash="dash", line_color="#5a6480")
    st.plotly_chart(fig2, use_container_width=True)

with tab_team:
    min_team_games = st.slider("Minimum home games per team", min_value=3, max_value=30, value=10, step=1)

    team_rows = []
    for lg in sel_leagues:
        g_lg = games[games["league"] == lg].copy()
        if "home_team" not in g_lg.columns:
            continue
        for team, g_team in g_lg.groupby("home_team"):
            row = build_posterior_row(g_team, label=str(team), league_key=lg, team_name=str(team))
            if row and row["games"] >= min_team_games:
                row["league_abbr"] = LEAGUES[lg]["abbr"]
                team_rows.append(row)

    if not team_rows:
        st.info("No teams have enough home games for the selected filters and threshold.")
        st.stop()

    tdf = pd.DataFrame(team_rows).sort_values("goal_edge_post", ascending=False)

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Teams modeled", len(tdf))
    with m2:
        st.metric("Avg team posterior home goal edge", f"{tdf['goal_edge_post'].mean():.2f}")
    with m3:
        st.metric("Avg team posterior home win prob", f"{(tdf['home_win_post'].mean()*100):.1f}%")

    st.markdown("### Team Home Advantage Table")
    tcols = [
        "league_abbr", "label", "games", "goal_edge_post", "goal_edge_low", "goal_edge_high",
        "home_win_post", "home_win_low", "home_win_high", "home_wins", "away_wins", "draws"
    ]
    st.dataframe(
        tdf[tcols].rename(
            columns={
                "league_abbr": "League",
                "label": "Team",
                "games": "Home Games",
                "goal_edge_post": "Post Goal Edge",
                "goal_edge_low": "Goal Edge CI Low",
                "goal_edge_high": "Goal Edge CI High",
                "home_win_post": "Post Home Win Prob",
                "home_win_low": "Win Prob CI Low",
                "home_win_high": "Win Prob CI High",
                "home_wins": "Home W",
                "away_wins": "Home L",
                "draws": "Draw/OT",
            }
        ),
        hide_index=True,
        use_container_width=True,
        height=420,
    )

    top_n = st.slider("Teams in chart", min_value=8, max_value=40, value=20, step=2)
    top = tdf.head(top_n).copy()
    st.markdown("### Top Teams by Posterior Home Goal Edge")
    fig3 = px.bar(
        top.sort_values("goal_edge_post", ascending=True),
        x="goal_edge_post",
        y="label",
        color="league_abbr",
        orientation="h",
        template="plotly_dark",
        labels={"goal_edge_post": "Posterior home goal edge", "label": ""},
    )
    fig3.update_layout(**PLOTLY_LAYOUT, height=520)
    fig3.update_yaxes(gridcolor="#1e2535")
    fig3.add_vline(x=0, line_dash="dash", line_color="#5a6480")
    st.plotly_chart(fig3, use_container_width=True)

st.caption(
    "Interpretation: posterior values combine your prior settings with observed match results. "
    "Change prior controls above to stress-test assumptions."
)

st.markdown("### Metric Definitions")
st.markdown(
    "- `Post Goal Edge`: Bayesian posterior mean of `(home goals - away goals)`. Positive means stronger home advantage.\n"
    "- `Goal Edge CI Low / High`: 95% credible interval for home goal edge.\n"
    "- `Post Home Win Prob`: Bayesian posterior mean probability that home team wins (draws excluded from W/L).\n"
    "- `Win Prob CI Low / High`: 95% credible interval for home win probability.\n"
    "- `Home W / Away W` (league tab): count of decisive games won by home/away side in filtered sample.\n"
    "- `Home W / Home L` (team tab): team home-game decisive wins/losses in filtered sample.\n"
    "- `Draw/OT`: non-decisive result count in this simplified W/L framing.\n"
    "- `Games` / `Home Games`: number of matches used for that row's estimate after filters.\n"
    "- `Avg posterior ...` cards: simple arithmetic mean of row-level posterior metrics currently shown."
)
