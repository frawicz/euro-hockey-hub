import streamlit as st
import sys
from pathlib import Path
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
)
from utils.poisson_models import (
    build_prior_config,
    fit_league_models,
    league_team_summary,
    predict_matchup,
    score_matrix_probs,
    model_diagnostics,
)

st.set_page_config(page_title="Poisson Goal Models · Euro Hockey Hub", page_icon="🏒", layout="wide")

sel_leagues, sel_season, sel_phase = build_sidebar()

st.title("Poisson Goal Models")
st.caption("Log-linear goal models by league: frequentist Poisson GLM plus Bayesian MAP with Gaussian priors")

games_raw = load_table("games", sel_leagues)
games = filter_by_phase(filter_by_season(games_raw, sel_season), sel_phase)

if games.empty:
    st.info("No game data available for selected filters.")
    st.stop()

st.markdown(
    "This page fits one model per league on team goals scored. "
    "The Bayesian version here is an approximate Bayesian fit: Gaussian priors + MAP estimation + Laplace posterior approximation."
)

with st.expander("Model Controls", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        min_games_per_team = st.slider(
            "Min games per team",
            min_value=2,
            max_value=20,
            value=6,
            step=1,
            help="Iteratively removes teams below this threshold before fitting.",
        )
    with c2:
        max_goals_grid = st.slider(
            "Matchup score grid max goals",
            min_value=6,
            max_value=12,
            value=10,
            step=1,
            help="Upper goal count used when approximating win/draw probabilities from score grids.",
        )
    with c3:
        show_full_team_table = st.checkbox("Show full team table", value=False)

with st.expander("Bayesian Prior Controls", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        prior_baseline_goals = st.slider(
            "Prior mean: baseline goals",
            min_value=1.0,
            max_value=4.5,
            value=2.7,
            step=0.1,
            help="Prior mean for the baseline team's away scoring rate in goals per team-game.",
        )
        prior_baseline_sd = st.slider(
            "Prior SD: baseline log-rate",
            min_value=0.05,
            max_value=1.0,
            value=0.45,
            step=0.05,
        )
    with c2:
        prior_home_mult = st.slider(
            "Prior mean: home multiplier",
            min_value=0.85,
            max_value=1.35,
            value=1.08,
            step=0.01,
            help="Prior mean for the multiplicative home-ice effect.",
        )
        prior_home_sd = st.slider(
            "Prior SD: home log-effect",
            min_value=0.05,
            max_value=0.6,
            value=0.18,
            step=0.01,
        )
    with c3:
        prior_attack_sd = st.slider(
            "Prior SD: attack effects",
            min_value=0.05,
            max_value=1.0,
            value=0.30,
            step=0.05,
        )
        prior_defense_sd = st.slider(
            "Prior SD: defense effects",
            min_value=0.05,
            max_value=1.0,
            value=0.30,
            step=0.05,
        )

prior_config = build_prior_config(
    baseline_goals_mean=prior_baseline_goals,
    baseline_log_sd=prior_baseline_sd,
    home_multiplier_mean=prior_home_mult,
    home_log_sd=prior_home_sd,
    attack_sd=prior_attack_sd,
    defense_sd=prior_defense_sd,
)

results = {}
failures = []
for lg in sel_leagues:
    try:
        bundle = fit_league_models(
            games,
            league=lg,
            prior_config=prior_config,
            min_games_per_team=min_games_per_team,
        )
        results[lg] = {
            "bundle": bundle,
            "diag_freq": model_diagnostics(bundle, mode="frequentist"),
            "diag_bayes": model_diagnostics(bundle, mode="bayes"),
            "teams_freq": league_team_summary(bundle, mode="frequentist"),
            "teams_bayes": league_team_summary(bundle, mode="bayes"),
        }
    except Exception as exc:
        failures.append((lg, str(exc)))

if failures:
    for lg, msg in failures:
        st.warning(f"{LEAGUES.get(lg, {}).get('abbr', lg)} model skipped: {msg}")

if not results:
    st.info("No league had enough clean score data to fit the models with the current filters.")
    st.stop()

summary_rows = []
for lg, payload in results.items():
    diag_freq = payload["diag_freq"]
    diag_bayes = payload["diag_bayes"]
    summary_rows.append(
        {
            "league": LEAGUES[lg]["abbr"],
            "league_key": lg,
            "games": diag_freq["n_games"],
            "teams": diag_freq["n_teams"],
            "mean_goals": round(diag_freq["mean_goals"] * 2, 2),
            "freq_home_mult": round(diag_freq["home_multiplier"], 3),
            "bayes_home_mult": round(diag_bayes["home_multiplier"], 3),
            "freq_overdisp": round(diag_freq["pearson_overdispersion"], 3),
            "bayes_overdisp": round(diag_bayes["pearson_overdispersion"], 3),
        }
    )

summary = pd.DataFrame(summary_rows).sort_values("league")

st.markdown("### League Model Summary")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Leagues fitted", len(summary))
with c2:
    st.metric("Games used", f"{int(summary['games'].sum()):,}")
with c3:
    st.metric("Avg frequentist home multiplier", f"{summary['freq_home_mult'].mean():.3f}")
with c4:
    st.metric("Avg Bayesian home multiplier", f"{summary['bayes_home_mult'].mean():.3f}")

st.dataframe(
    summary.rename(
        columns={
            "league": "League",
            "games": "Games",
            "teams": "Teams",
            "mean_goals": "Avg goals/game",
            "freq_home_mult": "Freq home mult",
            "bayes_home_mult": "Bayes home mult",
            "freq_overdisp": "Freq overdisp",
            "bayes_overdisp": "Bayes overdisp",
        }
    ).drop(columns=["league_key"]),
    hide_index=True,
    use_container_width=True,
)

tabs = st.tabs([LEAGUES[lg]["abbr"] for lg in results.keys()])

for tab, lg in zip(tabs, results.keys()):
    payload = results[lg]
    bundle = payload["bundle"]
    diag_freq = payload["diag_freq"]
    diag_bayes = payload["diag_bayes"]
    teams_freq = payload["teams_freq"]
    teams_bayes = payload["teams_bayes"]

    with tab:
        st.markdown(
            f"### {LEAGUES[lg]['label']}\n\n"
            f"Baseline team for dummy coding: `{bundle.baseline_team}`. "
            f"The Bayesian fit uses shrinkage toward league-average attack/defense."
        )

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Games modeled", diag_freq["n_games"])
        with m2:
            st.metric("Teams modeled", diag_freq["n_teams"])
        with m3:
            st.metric("Freq home multiplier", f"{diag_freq['home_multiplier']:.3f}")
        with m4:
            st.metric("Bayes home multiplier", f"{diag_bayes['home_multiplier']:.3f}")

        if not bundle.bayes_success:
            st.warning(f"Bayesian optimizer warning: {bundle.bayes_message}")

        compare = teams_freq.merge(
            teams_bayes,
            on=["team", "games", "is_baseline_team"],
            how="inner",
            suffixes=("_freq", "_bayes"),
        )
        compare["attack_gap"] = compare["attack_mult_bayes"] - compare["attack_mult_freq"]
        compare["defense_gap"] = compare["defense_suppression_mult_bayes"] - compare["defense_suppression_mult_freq"]

        chart_df = compare[["team", "net_rating_freq", "net_rating_bayes"]].melt(
            id_vars="team",
            var_name="model",
            value_name="net_rating",
        )
        chart_df["model"] = chart_df["model"].map(
            {"net_rating_freq": "Frequentist", "net_rating_bayes": "Bayesian"}
        )

        st.markdown("### Team Net Ratings")
        fig = px.bar(
            chart_df.sort_values(["model", "net_rating"], ascending=[True, False]),
            x="team",
            y="net_rating",
            color="model",
            barmode="group",
            color_discrete_map={"Frequentist": "#4fc3f7", "Bayesian": "#ffb703"},
            template="plotly_dark",
            labels={"team": "", "net_rating": "Attack - defensive allowance"},
        )
        fig.update_layout(**PLOTLY_LAYOUT, height=360, xaxis_tickangle=-40)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Team Strength Comparison")
        show_cols = [
            "team",
            "games",
            "attack_mult_freq",
            "attack_mult_bayes",
            "defense_suppression_mult_freq",
            "defense_suppression_mult_bayes",
            "net_rating_freq",
            "net_rating_bayes",
            "attack_gap",
            "defense_gap",
        ]
        display = compare[show_cols].rename(
            columns={
                "team": "Team",
                "games": "Games",
                "attack_mult_freq": "Freq attack mult",
                "attack_mult_bayes": "Bayes attack mult",
                "defense_suppression_mult_freq": "Freq defense mult",
                "defense_suppression_mult_bayes": "Bayes defense mult",
                "net_rating_freq": "Freq net",
                "net_rating_bayes": "Bayes net",
                "attack_gap": "Bayes-Freq attack",
                "defense_gap": "Bayes-Freq defense",
            }
        )
        if not show_full_team_table:
            display = display.head(10)
        st.dataframe(display.round(3), hide_index=True, use_container_width=True)

        st.markdown("### Matchup Explorer")
        team_options = compare["team"].sort_values().tolist()
        c1, c2 = st.columns(2)
        with c1:
            matchup_home = st.selectbox(
                "Home team",
                options=team_options,
                index=0,
                key=f"home_team_{lg}",
            )
        with c2:
            away_default = 1 if len(team_options) > 1 else 0
            matchup_away = st.selectbox(
                "Away team",
                options=team_options,
                index=away_default,
                key=f"away_team_{lg}",
            )

        if matchup_home == matchup_away:
            st.info("Choose two different teams to generate matchup probabilities.")
        else:
            pred_freq = predict_matchup(bundle, matchup_home, matchup_away, mode="frequentist")
            pred_bayes = predict_matchup(bundle, matchup_home, matchup_away, mode="bayes")
            _, probs_freq = score_matrix_probs(pred_freq["lambda_home"], pred_freq["lambda_away"], max_goals=max_goals_grid)
            _, probs_bayes = score_matrix_probs(pred_bayes["lambda_home"], pred_bayes["lambda_away"], max_goals=max_goals_grid)

            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                st.metric("Freq expected home goals", f"{pred_freq['lambda_home']:.2f}")
            with mc2:
                st.metric("Freq expected away goals", f"{pred_freq['lambda_away']:.2f}")
            with mc3:
                st.metric("Bayes expected home goals", f"{pred_bayes['lambda_home']:.2f}")
            with mc4:
                st.metric("Bayes expected away goals", f"{pred_bayes['lambda_away']:.2f}")

            prob_table = pd.DataFrame(
                [
                    {
                        "Model": "Frequentist",
                        "Home win": probs_freq["home_win"],
                        "Draw": probs_freq["draw"],
                        "Away win": probs_freq["away_win"],
                        "Grid mass": probs_freq["captured_mass"],
                    },
                    {
                        "Model": "Bayesian",
                        "Home win": probs_bayes["home_win"],
                        "Draw": probs_bayes["draw"],
                        "Away win": probs_bayes["away_win"],
                        "Grid mass": probs_bayes["captured_mass"],
                    },
                ]
            )
            st.dataframe(prob_table.round(3), hide_index=True, use_container_width=True)

            st.caption(
                "Interpretation: attack multipliers above 1.0 mean stronger scoring than the league baseline. "
                "Defense multipliers above 1.0 mean stronger goal suppression."
            )
