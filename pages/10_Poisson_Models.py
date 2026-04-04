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
    score_matrix_probs_dc,
    estimate_dixon_coles_rho,
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
    "The Bayesian version is an approximate Bayesian fit: Gaussian priors + MAP estimation + Laplace posterior approximation."
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

with st.expander("Extensions", expanded=False):
    ec1, ec2, ec3 = st.columns(3)
    with ec1:
        use_nb = st.checkbox(
            "Negative Binomial (NB2)",
            value=False,
            help="Fits an NB2 model (Var = μ + α·μ²) alongside Poisson to handle overdispersion. "
            "Slower to fit. α close to 0 means Poisson is adequate.",
        )
    with ec2:
        use_dc = st.checkbox(
            "Dixon-Coles correction",
            value=False,
            help="Adjusts joint score-grid probabilities for 0-0, 1-0, 0-1, and 1-1 outcomes "
            "via a correlation parameter ρ estimated from the fitted model.",
        )
    with ec3:
        use_decay = st.checkbox(
            "Time decay",
            value=False,
            help="Weights recent games more heavily. Games from `half-life` days ago receive "
            "half the weight of today's games.",
        )
        decay_hl: float | None = None
        if use_decay:
            decay_hl = float(
                st.slider(
                    "Half-life (days)",
                    min_value=20,
                    max_value=365,
                    value=90,
                    step=10,
                    help="Number of days after which a game's weight is halved.",
                )
            )

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
            decay_half_life_days=decay_hl,
            fit_neg_binom=use_nb,
        )
        # Dixon-Coles rho is estimated post-hoc (doesn't affect the linear predictor)
        if use_dc:
            bundle.rho_freq = estimate_dixon_coles_rho(bundle, mode="frequentist")
            bundle.rho_bayes = estimate_dixon_coles_rho(bundle, mode="bayes")
            if use_nb and bundle.nb_success:
                bundle.rho_nb = estimate_dixon_coles_rho(bundle, mode="neg_binom")

        payload: dict = {
            "bundle": bundle,
            "diag_freq": model_diagnostics(bundle, mode="frequentist"),
            "diag_bayes": model_diagnostics(bundle, mode="bayes"),
            "teams_freq": league_team_summary(bundle, mode="frequentist"),
            "teams_bayes": league_team_summary(bundle, mode="bayes"),
        }
        if use_nb and bundle.nb_success:
            payload["diag_nb"] = model_diagnostics(bundle, mode="neg_binom")
            payload["teams_nb"] = league_team_summary(bundle, mode="neg_binom")

        results[lg] = payload
    except Exception as exc:
        failures.append((lg, str(exc)))

if failures:
    for lg, msg in failures:
        st.warning(f"{LEAGUES.get(lg, {}).get('abbr', lg)} model skipped: {msg}")

if not results:
    st.info("No league had enough clean score data to fit the models with the current filters.")
    st.stop()

# ── League summary table ──────────────────────────────────────────────────────
summary_rows = []
for lg, payload in results.items():
    diag_freq = payload["diag_freq"]
    diag_bayes = payload["diag_bayes"]
    row = {
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
    if use_nb and "diag_nb" in payload:
        diag_nb = payload["diag_nb"]
        row["nb_alpha"] = round(diag_nb.get("nb_alpha", float("nan")), 4)
        row["nb_overdisp"] = round(diag_nb["pearson_overdispersion"], 3)
    summary_rows.append(row)

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

rename_map = {
    "league": "League",
    "games": "Games",
    "teams": "Teams",
    "mean_goals": "Avg goals/game",
    "freq_home_mult": "Freq home mult",
    "bayes_home_mult": "Bayes home mult",
    "freq_overdisp": "Freq overdisp",
    "bayes_overdisp": "Bayes overdisp",
    "nb_alpha": "NB alpha",
    "nb_overdisp": "NB overdisp",
}
display_cols = [c for c in rename_map if c in summary.columns and c != "league_key"]
st.dataframe(
    summary[display_cols].rename(columns=rename_map),
    hide_index=True,
    use_container_width=True,
)

# ── Per-league tabs ───────────────────────────────────────────────────────────
tabs = st.tabs([LEAGUES[lg]["abbr"] for lg in results.keys()])

for tab, lg in zip(tabs, results.keys()):
    payload = results[lg]
    bundle = payload["bundle"]
    diag_freq = payload["diag_freq"]
    diag_bayes = payload["diag_bayes"]
    teams_freq = payload["teams_freq"]
    teams_bayes = payload["teams_bayes"]
    teams_nb = payload.get("teams_nb")
    diag_nb = payload.get("diag_nb")

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

        if use_nb and not bundle.nb_success:
            st.warning(f"NB2 optimizer did not converge: {bundle.nb_message}")
        if not bundle.bayes_success:
            st.warning(f"Bayesian optimizer warning: {bundle.bayes_message}")

        # Extra diagnostics row when extensions are on
        ext_cols = []
        if use_nb and diag_nb:
            ext_cols.append(("NB alpha (dispersion)", f"{diag_nb.get('nb_alpha', float('nan')):.4f}"))
            ext_cols.append(("NB Pearson overdisp", f"{diag_nb['pearson_overdispersion']:.3f}"))
        if use_dc:
            if bundle.rho_freq is not None:
                ext_cols.append(("DC ρ (frequentist)", f"{bundle.rho_freq:.4f}"))
            if bundle.rho_bayes is not None:
                ext_cols.append(("DC ρ (Bayesian)", f"{bundle.rho_bayes:.4f}"))
            if use_nb and bundle.rho_nb is not None:
                ext_cols.append(("DC ρ (NB2)", f"{bundle.rho_nb:.4f}"))
        if use_decay and bundle.decay_weights is not None:
            ext_cols.append(("Decay min weight", f"{bundle.decay_weights.min():.3f}"))
        if ext_cols:
            mcols = st.columns(len(ext_cols))
            for col, (label, val) in zip(mcols, ext_cols):
                with col:
                    st.metric(label, val)

        # ── Net ratings chart ─────────────────────────────────────────────────
        compare = teams_freq.merge(
            teams_bayes,
            on=["team", "games", "is_baseline_team"],
            how="inner",
            suffixes=("_freq", "_bayes"),
        )
        if teams_nb is not None:
            compare = compare.merge(
                teams_nb[["team", "net_rating", "attack_mult", "defense_suppression_mult"]].rename(
                    columns={
                        "net_rating": "net_rating_nb",
                        "attack_mult": "attack_mult_nb",
                        "defense_suppression_mult": "defense_suppression_mult_nb",
                    }
                ),
                on="team",
                how="left",
            )

        compare["attack_gap"] = compare["attack_mult_bayes"] - compare["attack_mult_freq"]
        compare["defense_gap"] = compare["defense_suppression_mult_bayes"] - compare["defense_suppression_mult_freq"]

        melt_cols = ["team", "net_rating_freq", "net_rating_bayes"]
        model_label_map = {"net_rating_freq": "Frequentist", "net_rating_bayes": "Bayesian"}
        color_map = {"Frequentist": "#4fc3f7", "Bayesian": "#ffb703"}
        if "net_rating_nb" in compare.columns:
            melt_cols.append("net_rating_nb")
            model_label_map["net_rating_nb"] = "Neg. Binomial"
            color_map["Neg. Binomial"] = "#06d6a0"

        chart_df = compare[melt_cols].melt(id_vars="team", var_name="model", value_name="net_rating")
        chart_df["model"] = chart_df["model"].map(model_label_map)

        st.markdown("### Team Net Ratings")
        fig = px.bar(
            chart_df.sort_values(["model", "net_rating"], ascending=[True, False]),
            x="team",
            y="net_rating",
            color="model",
            barmode="group",
            color_discrete_map=color_map,
            template="plotly_dark",
            labels={"team": "", "net_rating": "Attack - defensive allowance"},
        )
        fig.update_layout(**PLOTLY_LAYOUT, height=360, xaxis_tickangle=-40)
        st.plotly_chart(fig, use_container_width=True)

        # ── Team strength table ───────────────────────────────────────────────
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
        col_rename = {
            "team": "Team",
            "games": "Games",
            "attack_mult_freq": "Freq attack",
            "attack_mult_bayes": "Bayes attack",
            "defense_suppression_mult_freq": "Freq defense",
            "defense_suppression_mult_bayes": "Bayes defense",
            "net_rating_freq": "Freq net",
            "net_rating_bayes": "Bayes net",
            "attack_gap": "Bayes-Freq attack",
            "defense_gap": "Bayes-Freq defense",
        }
        if "attack_mult_nb" in compare.columns:
            show_cols += ["attack_mult_nb", "defense_suppression_mult_nb", "net_rating_nb"]
            col_rename.update({
                "attack_mult_nb": "NB attack",
                "defense_suppression_mult_nb": "NB defense",
                "net_rating_nb": "NB net",
            })
        present_cols = [c for c in show_cols if c in compare.columns]
        display = compare[present_cols].rename(columns=col_rename)
        if not show_full_team_table:
            display = display.head(10)
        st.dataframe(display.round(3), hide_index=True, use_container_width=True)

        # ── Matchup Explorer ──────────────────────────────────────────────────
        st.markdown("### Matchup Explorer")
        team_options = compare["team"].sort_values().tolist()
        mc1, mc2 = st.columns(2)
        with mc1:
            matchup_home = st.selectbox(
                "Home team",
                options=team_options,
                index=0,
                key=f"home_team_{lg}",
            )
        with mc2:
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
            # Build model list
            model_modes = [("Frequentist", "frequentist"), ("Bayesian", "bayes")]
            if use_nb and bundle.nb_success:
                model_modes.append(("Neg. Binomial", "neg_binom"))

            # Compute predictions for all base models
            matchup_rows = []
            for model_label, mode_key in model_modes:
                pred = predict_matchup(bundle, matchup_home, matchup_away, mode=mode_key)
                _, probs = score_matrix_probs(pred["lambda_home"], pred["lambda_away"], max_goals=max_goals_grid)
                matchup_rows.append({
                    "Model": model_label,
                    "xG home": round(pred["lambda_home"], 3),
                    "xG away": round(pred["lambda_away"], 3),
                    "Home win": round(probs["home_win"], 3),
                    "Draw": round(probs["draw"], 3),
                    "Away win": round(probs["away_win"], 3),
                    "Grid mass": round(probs["captured_mass"], 4),
                    "_dc": False,
                })

                # Dixon-Coles corrected row for this model
                if use_dc:
                    rho_map = {"frequentist": bundle.rho_freq, "bayes": bundle.rho_bayes, "neg_binom": bundle.rho_nb}
                    rho = rho_map.get(mode_key)
                    if rho is not None:
                        _, probs_dc = score_matrix_probs_dc(
                            pred["lambda_home"], pred["lambda_away"], rho, max_goals=max_goals_grid
                        )
                        matchup_rows.append({
                            "Model": f"{model_label} + DC (ρ={rho:.4f})",
                            "xG home": round(pred["lambda_home"], 3),
                            "xG away": round(pred["lambda_away"], 3),
                            "Home win": round(probs_dc["home_win"], 3),
                            "Draw": round(probs_dc["draw"], 3),
                            "Away win": round(probs_dc["away_win"], 3),
                            "Grid mass": round(probs_dc["captured_mass"], 4),
                            "_dc": True,
                        })

            prob_table = pd.DataFrame(matchup_rows).drop(columns=["_dc"])
            st.dataframe(prob_table, hide_index=True, use_container_width=True)

            st.caption(
                "xG = expected goals (λ from the log-linear predictor). "
                "Attack multipliers above 1.0 mean stronger scoring than the league baseline. "
                "Defense multipliers above 1.0 mean stronger goal suppression. "
                + ("DC correction adjusts the (0-0, 1-0, 0-1, 1-1) score cells for low-score correlation. " if use_dc else "")
                + ("NB2 alpha > 0 indicates overdispersion beyond Poisson; larger α = more spread. " if use_nb else "")
            )
