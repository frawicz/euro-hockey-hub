"""
Poisson goal models for league-level hockey scoring analysis.

This module provides:
- a frequentist log-linear Poisson regression
- a Bayesian MAP fit with Gaussian priors and a Laplace approximation
"""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd
import scipy.optimize as opt
import scipy.stats as st
import statsmodels.api as sm


@dataclass
class PriorConfig:
    intercept_mean: float
    intercept_sd: float
    home_mean: float
    home_sd: float
    attack_sd: float
    defense_sd: float


@dataclass
class ModelBundle:
    league: str
    games_used: pd.DataFrame
    long_df: pd.DataFrame
    teams: list[str]
    baseline_team: str
    design_matrix: pd.DataFrame
    response: np.ndarray
    frequentist_params: pd.Series
    frequentist_cov: pd.DataFrame
    frequentist_mu: np.ndarray
    frequentist_result: object
    bayes_params: pd.Series
    bayes_cov: pd.DataFrame
    bayes_mu: np.ndarray
    bayes_success: bool
    bayes_message: str
    prior_config: PriorConfig


def build_prior_config(
    baseline_goals_mean: float = 2.7,
    baseline_log_sd: float = 0.45,
    home_multiplier_mean: float = 1.08,
    home_log_sd: float = 0.18,
    attack_sd: float = 0.30,
    defense_sd: float = 0.30,
) -> PriorConfig:
    return PriorConfig(
        intercept_mean=float(math.log(max(baseline_goals_mean, 0.05))),
        intercept_sd=float(max(baseline_log_sd, 0.02)),
        home_mean=float(math.log(max(home_multiplier_mean, 0.2))),
        home_sd=float(max(home_log_sd, 0.02)),
        attack_sd=float(max(attack_sd, 0.02)),
        defense_sd=float(max(defense_sd, 0.02)),
    )


def _coerce_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["home_score"] = pd.to_numeric(out.get("home_score"), errors="coerce")
    out["away_score"] = pd.to_numeric(out.get("away_score"), errors="coerce")
    return out


def _iteratively_filter_teams(games: pd.DataFrame, min_games_per_team: int) -> pd.DataFrame:
    if games.empty or min_games_per_team <= 1:
        return games

    out = games.copy()
    while True:
        counts = pd.concat([out["home_team"], out["away_team"]], ignore_index=True).value_counts()
        keep = counts[counts >= min_games_per_team].index
        nxt = out[out["home_team"].isin(keep) & out["away_team"].isin(keep)].copy()
        if len(nxt) == len(out):
            return nxt
        out = nxt
        if out.empty:
            return out


def prepare_games_for_model(games: pd.DataFrame, league: str, min_games_per_team: int = 6) -> pd.DataFrame:
    cols = ["league", "game_id", "date", "home_team", "away_team", "home_score", "away_score"]
    present = [c for c in cols if c in games.columns]
    out = games[present].copy()
    if "league" in out.columns:
        out = out[out["league"] == league].copy()
    out = _coerce_scores(out)
    out = out.dropna(subset=["home_team", "away_team", "home_score", "away_score"]).copy()
    out = out[out["home_team"] != out["away_team"]].copy()
    out = _iteratively_filter_teams(out, min_games_per_team=min_games_per_team)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.reset_index(drop=True)


def build_long_format(games: pd.DataFrame) -> pd.DataFrame:
    home = games[["game_id", "date", "home_team", "away_team", "home_score"]].copy()
    home.columns = ["game_id", "date", "team", "opponent", "goals"]
    home["is_home"] = 1

    away = games[["game_id", "date", "away_team", "home_team", "away_score"]].copy()
    away.columns = ["game_id", "date", "team", "opponent", "goals"]
    away["is_home"] = 0

    out = pd.concat([home, away], ignore_index=True)
    out["goals"] = pd.to_numeric(out["goals"], errors="coerce")
    out["is_home"] = pd.to_numeric(out["is_home"], errors="coerce").fillna(0)
    return out.dropna(subset=["team", "opponent", "goals"]).reset_index(drop=True)


def build_design_matrix(long_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], str]:
    teams = sorted(pd.unique(pd.concat([long_df["team"], long_df["opponent"]], ignore_index=True)).tolist())
    baseline_team = teams[0]

    attack = pd.get_dummies(long_df["team"], prefix="att", dtype=float)
    defense = pd.get_dummies(long_df["opponent"], prefix="def", dtype=float)
    attack = attack.drop(columns=[f"att_{baseline_team}"], errors="ignore")
    defense = defense.drop(columns=[f"def_{baseline_team}"], errors="ignore")

    x = pd.concat(
        [
            pd.DataFrame({"intercept": np.ones(len(long_df), dtype=float), "home": long_df["is_home"].astype(float)}),
            attack,
            defense,
        ],
        axis=1,
    )
    return x, teams, baseline_team


def _series_from_params(values: np.ndarray, columns: list[str]) -> pd.Series:
    return pd.Series(np.asarray(values, dtype=float), index=columns, dtype=float)


def _cov_from_matrix(values: np.ndarray, columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(np.asarray(values, dtype=float), index=columns, columns=columns)


def fit_frequentist_poisson(x: pd.DataFrame, y: np.ndarray):
    model = sm.GLM(y, x, family=sm.families.Poisson())
    result = model.fit()
    params = _series_from_params(result.params, list(x.columns))
    cov = _cov_from_matrix(result.cov_params(), list(x.columns))
    mu = np.asarray(result.predict(x), dtype=float)
    return params, cov, mu, result


def fit_bayesian_poisson_map(
    x: pd.DataFrame,
    y: np.ndarray,
    prior: PriorConfig,
    init_params: np.ndarray | None = None,
):
    cols = list(x.columns)
    n = len(cols)

    prior_mean = np.zeros(n, dtype=float)
    prior_sd = np.full(n, 1_000_000.0, dtype=float)

    prior_mean[cols.index("intercept")] = prior.intercept_mean
    prior_sd[cols.index("intercept")] = prior.intercept_sd
    prior_mean[cols.index("home")] = prior.home_mean
    prior_sd[cols.index("home")] = prior.home_sd

    for i, col in enumerate(cols):
        if col.startswith("att_"):
            prior_sd[i] = prior.attack_sd
        elif col.startswith("def_"):
            prior_sd[i] = prior.defense_sd

    precision = 1.0 / np.square(prior_sd)
    x_mat = x.to_numpy(dtype=float)
    y_vec = np.asarray(y, dtype=float)

    if init_params is None:
        init_params = np.zeros(n, dtype=float)
        init_params[cols.index("intercept")] = prior.intercept_mean
        init_params[cols.index("home")] = prior.home_mean
    else:
        init_params = np.asarray(init_params, dtype=float)

    def objective(beta):
        eta = x_mat @ beta
        mu = np.exp(np.clip(eta, -20, 20))
        neg_log_like = np.sum(mu - y_vec * eta)
        penalty = 0.5 * np.sum(precision * np.square(beta - prior_mean))
        return float(neg_log_like + penalty)

    def gradient(beta):
        eta = x_mat @ beta
        mu = np.exp(np.clip(eta, -20, 20))
        grad = x_mat.T @ (mu - y_vec) + precision * (beta - prior_mean)
        return np.asarray(grad, dtype=float)

    opt_res = opt.minimize(objective, init_params, jac=gradient, method="BFGS")
    beta_hat = np.asarray(opt_res.x, dtype=float)
    eta_hat = x_mat @ beta_hat
    mu_hat = np.exp(np.clip(eta_hat, -20, 20))

    weighted_x = x_mat * mu_hat[:, None]
    hess = x_mat.T @ weighted_x + np.diag(precision)
    cov = np.linalg.pinv(hess)

    params = _series_from_params(beta_hat, cols)
    cov_df = _cov_from_matrix(cov, cols)
    return params, cov_df, mu_hat, bool(opt_res.success), str(opt_res.message)


def fit_league_models(
    games: pd.DataFrame,
    league: str,
    prior_config: PriorConfig,
    min_games_per_team: int = 6,
) -> ModelBundle:
    league_games = prepare_games_for_model(games, league=league, min_games_per_team=min_games_per_team)
    if league_games.empty:
        raise ValueError("No usable games available after filtering.")

    long_df = build_long_format(league_games)
    x, teams, baseline_team = build_design_matrix(long_df)
    y = long_df["goals"].to_numpy(dtype=float)

    freq_params, freq_cov, freq_mu, freq_res = fit_frequentist_poisson(x, y)
    bayes_params, bayes_cov, bayes_mu, bayes_success, bayes_message = fit_bayesian_poisson_map(
        x,
        y,
        prior=prior_config,
        init_params=freq_params.to_numpy(dtype=float),
    )

    return ModelBundle(
        league=league,
        games_used=league_games,
        long_df=long_df,
        teams=teams,
        baseline_team=baseline_team,
        design_matrix=x,
        response=y,
        frequentist_params=freq_params,
        frequentist_cov=freq_cov,
        frequentist_mu=freq_mu,
        frequentist_result=freq_res,
        bayes_params=bayes_params,
        bayes_cov=bayes_cov,
        bayes_mu=bayes_mu,
        bayes_success=bayes_success,
        bayes_message=bayes_message,
        prior_config=prior_config,
    )


def _team_coef(params: pd.Series, prefix: str, team: str, baseline_team: str) -> float:
    if team == baseline_team:
        return 0.0
    return float(params.get(f"{prefix}_{team}", 0.0))


def league_team_summary(bundle: ModelBundle, mode: str = "frequentist") -> pd.DataFrame:
    if mode == "frequentist":
        params = bundle.frequentist_params
    else:
        params = bundle.bayes_params

    rows = []
    game_counts = pd.concat([bundle.games_used["home_team"], bundle.games_used["away_team"]], ignore_index=True).value_counts()
    for team in bundle.teams:
        attack = _team_coef(params, "att", team, bundle.baseline_team)
        defense_allow = _team_coef(params, "def", team, bundle.baseline_team)
        rows.append(
            {
                "team": team,
                "games": int(game_counts.get(team, 0)),
                "attack_log": attack,
                "attack_mult": math.exp(attack),
                "defense_allow_log": defense_allow,
                "defense_concede_mult": math.exp(defense_allow),
                "defense_suppression_log": -defense_allow,
                "defense_suppression_mult": math.exp(-defense_allow),
                "net_rating": attack - defense_allow,
                "is_baseline_team": team == bundle.baseline_team,
            }
        )
    return pd.DataFrame(rows).sort_values("net_rating", ascending=False).reset_index(drop=True)


def predict_matchup(bundle: ModelBundle, home_team: str, away_team: str, mode: str = "frequentist") -> dict[str, float]:
    params = bundle.frequentist_params if mode == "frequentist" else bundle.bayes_params
    intercept = float(params["intercept"])
    home_coef = float(params["home"])

    home_eta = (
        intercept
        + home_coef
        + _team_coef(params, "att", home_team, bundle.baseline_team)
        + _team_coef(params, "def", away_team, bundle.baseline_team)
    )
    away_eta = (
        intercept
        + _team_coef(params, "att", away_team, bundle.baseline_team)
        + _team_coef(params, "def", home_team, bundle.baseline_team)
    )

    return {
        "lambda_home": float(math.exp(home_eta)),
        "lambda_away": float(math.exp(away_eta)),
    }


def score_matrix_probs(lambda_home: float, lambda_away: float, max_goals: int = 10) -> tuple[pd.DataFrame, dict[str, float]]:
    home_range = np.arange(0, max_goals + 1)
    away_range = np.arange(0, max_goals + 1)
    home_p = st.poisson.pmf(home_range, lambda_home)
    away_p = st.poisson.pmf(away_range, lambda_away)
    mat = np.outer(home_p, away_p)

    rows = []
    for i, hg in enumerate(home_range):
        for j, ag in enumerate(away_range):
            rows.append({"home_goals": int(hg), "away_goals": int(ag), "prob": float(mat[i, j])})
    df = pd.DataFrame(rows)

    home_win = float(df[df["home_goals"] > df["away_goals"]]["prob"].sum())
    away_win = float(df[df["away_goals"] > df["home_goals"]]["prob"].sum())
    draw = float(df[df["home_goals"] == df["away_goals"]]["prob"].sum())
    mass = float(df["prob"].sum())

    return df, {
        "home_win": home_win,
        "away_win": away_win,
        "draw": draw,
        "captured_mass": mass,
    }


def model_diagnostics(bundle: ModelBundle, mode: str = "frequentist") -> dict[str, float]:
    if mode == "frequentist":
        mu = bundle.frequentist_mu
        params = bundle.frequentist_params
    else:
        mu = bundle.bayes_mu
        params = bundle.bayes_params

    y = bundle.response
    resid = y - mu
    pearson = np.sum(np.square(resid) / np.clip(mu, 1e-8, None))
    dof = max(len(y) - len(params), 1)

    return {
        "n_obs": int(len(y)),
        "n_games": int(len(bundle.games_used)),
        "n_teams": int(len(bundle.teams)),
        "mean_goals": float(np.mean(y)),
        "mean_fitted": float(np.mean(mu)),
        "pearson_overdispersion": float(pearson / dof),
        "home_multiplier": float(math.exp(params["home"])),
        "baseline_goals": float(math.exp(params["intercept"])),
    }
