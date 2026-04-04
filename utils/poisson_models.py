"""
Poisson goal models for league-level hockey scoring analysis.

This module provides:
- a frequentist log-linear Poisson regression
- a Bayesian MAP fit with Gaussian priors and a Laplace approximation
- a negative binomial (NB2) regression for overdispersed counts
- optional time-decay weighting by game recency
- Dixon-Coles score-correlation correction for low-scoring outcomes
"""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd
import scipy.optimize as opt
import scipy.stats as st
from scipy.special import gammaln
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
    # Negative Binomial (NB2)
    nb_params: pd.Series | None = None
    nb_cov: pd.DataFrame | None = None
    nb_mu: np.ndarray | None = None
    nb_alpha: float | None = None
    nb_success: bool = False
    nb_message: str = "not fitted"
    # Time decay
    decay_weights: np.ndarray | None = None
    # Dixon-Coles rho (set post-hoc via estimate_dixon_coles_rho)
    rho_freq: float | None = None
    rho_bayes: float | None = None
    rho_nb: float | None = None


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


def _compute_decay_weights(dates: pd.Series, half_life_days: float) -> np.ndarray:
    """Exponential recency weights: w_i = exp(-ln(2) / half_life * days_ago).

    Weights are normalised so they sum to n_obs, keeping likelihood scale stable.
    """
    decay_rate = math.log(2) / max(half_life_days, 1.0)
    ref = dates.max()
    days_ago = (ref - dates).dt.total_seconds() / 86400.0
    raw = np.exp(-decay_rate * days_ago.fillna(0.0).clip(lower=0.0).to_numpy(dtype=float))
    return raw * (len(raw) / raw.sum())


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


def _get_params(bundle: ModelBundle, mode: str) -> pd.Series:
    if mode == "neg_binom":
        if bundle.nb_params is None:
            raise ValueError("Negative binomial was not fitted for this bundle.")
        return bundle.nb_params
    if mode == "bayes":
        return bundle.bayes_params
    return bundle.frequentist_params


def _get_mu(bundle: ModelBundle, mode: str) -> np.ndarray:
    if mode == "neg_binom":
        if bundle.nb_mu is None:
            raise ValueError("Negative binomial was not fitted for this bundle.")
        return bundle.nb_mu
    if mode == "bayes":
        return bundle.bayes_mu
    return bundle.frequentist_mu


def fit_frequentist_poisson(
    x: pd.DataFrame,
    y: np.ndarray,
    weights: np.ndarray | None = None,
):
    if weights is not None:
        model = sm.GLM(y, x, family=sm.families.Poisson(), freq_weights=weights)
    else:
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
    weights: np.ndarray | None = None,
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
    w = np.ones(len(y_vec), dtype=float) if weights is None else np.asarray(weights, dtype=float)

    if init_params is None:
        init_params = np.zeros(n, dtype=float)
        init_params[cols.index("intercept")] = prior.intercept_mean
        init_params[cols.index("home")] = prior.home_mean
    else:
        init_params = np.asarray(init_params, dtype=float)

    def objective(beta):
        eta = x_mat @ beta
        mu = np.exp(np.clip(eta, -20, 20))
        neg_log_like = np.sum(w * (mu - y_vec * eta))
        penalty = 0.5 * np.sum(precision * np.square(beta - prior_mean))
        return float(neg_log_like + penalty)

    def gradient(beta):
        eta = x_mat @ beta
        mu = np.exp(np.clip(eta, -20, 20))
        grad = x_mat.T @ (w * (mu - y_vec)) + precision * (beta - prior_mean)
        return np.asarray(grad, dtype=float)

    opt_res = opt.minimize(objective, init_params, jac=gradient, method="BFGS")
    beta_hat = np.asarray(opt_res.x, dtype=float)
    eta_hat = x_mat @ beta_hat
    mu_hat = np.exp(np.clip(eta_hat, -20, 20))

    weighted_x = x_mat * (w * mu_hat)[:, None]
    hess = x_mat.T @ weighted_x + np.diag(precision)
    cov = np.linalg.pinv(hess)

    params = _series_from_params(beta_hat, cols)
    cov_df = _cov_from_matrix(cov, cols)
    return params, cov_df, mu_hat, bool(opt_res.success), str(opt_res.message)


def fit_negative_binomial(
    x: pd.DataFrame,
    y: np.ndarray,
    init_params: np.ndarray | None = None,
    weights: np.ndarray | None = None,
):
    """NB2 MLE via direct optimization.

    Variance model: Var(Y) = mu + alpha * mu^2.
    The internal parameter vector is [beta..., log_alpha] for unconstrained optimization.
    Returns regression params (same index as x.columns), fitted mu, and alpha.
    """
    cols = list(x.columns)
    n_reg = len(cols)
    x_mat = x.to_numpy(dtype=float)
    y_vec = np.asarray(y, dtype=float)
    w = np.ones(len(y_vec), dtype=float) if weights is None else np.asarray(weights, dtype=float)

    if init_params is None:
        p0 = np.zeros(n_reg + 1, dtype=float)
    else:
        p0 = np.append(np.asarray(init_params, dtype=float)[:n_reg], -2.0)  # start log_alpha=-2 -> alpha~0.13

    def neg_ll(theta: np.ndarray) -> float:
        beta = theta[:n_reg]
        alpha = float(np.exp(np.clip(theta[n_reg], -10, 4)))
        inv_a = 1.0 / alpha
        eta = x_mat @ beta
        mu = np.exp(np.clip(eta, -20, 20))
        ll = (
            gammaln(y_vec + inv_a)
            - gammaln(inv_a)
            + inv_a * np.log(np.clip(inv_a / (inv_a + mu), 1e-15, None))
            + y_vec * np.log(np.clip(mu / (inv_a + mu), 1e-15, None))
        )
        return -float(np.sum(w * ll))

    try:
        res = opt.minimize(neg_ll, p0, method="BFGS")
        beta_hat = np.asarray(res.x[:n_reg], dtype=float)
        alpha_hat = float(np.exp(np.clip(res.x[n_reg], -10, 4)))
        eta_hat = x_mat @ beta_hat
        mu_hat = np.exp(np.clip(eta_hat, -20, 20))
        # BFGS returns approximate inverse Hessian — extract regression block
        hess_inv = np.asarray(res.hess_inv, dtype=float)
        cov_beta = hess_inv[:n_reg, :n_reg]
        params = _series_from_params(beta_hat, cols)
        cov_df = _cov_from_matrix(cov_beta, cols)
        return params, cov_df, mu_hat, alpha_hat, bool(res.success), str(res.message)
    except Exception as exc:
        params = _series_from_params(np.zeros(n_reg), cols)
        cov_df = _cov_from_matrix(np.eye(n_reg), cols)
        mu_hat = np.ones(len(y_vec))
        return params, cov_df, mu_hat, 1.0, False, str(exc)


def fit_league_models(
    games: pd.DataFrame,
    league: str,
    prior_config: PriorConfig,
    min_games_per_team: int = 6,
    decay_half_life_days: float | None = None,
    fit_neg_binom: bool = False,
) -> ModelBundle:
    league_games = prepare_games_for_model(games, league=league, min_games_per_team=min_games_per_team)
    if league_games.empty:
        raise ValueError("No usable games available after filtering.")

    long_df = build_long_format(league_games)
    x, teams, baseline_team = build_design_matrix(long_df)
    y = long_df["goals"].to_numpy(dtype=float)

    # Time-decay weights (only when dates are available and valid)
    decay_weights: np.ndarray | None = None
    if decay_half_life_days is not None and "date" in long_df.columns:
        dates = pd.to_datetime(long_df["date"], errors="coerce")
        if dates.notna().any():
            decay_weights = _compute_decay_weights(dates, half_life_days=decay_half_life_days)

    freq_params, freq_cov, freq_mu, freq_res = fit_frequentist_poisson(x, y, weights=decay_weights)
    bayes_params, bayes_cov, bayes_mu, bayes_success, bayes_message = fit_bayesian_poisson_map(
        x,
        y,
        prior=prior_config,
        init_params=freq_params.to_numpy(dtype=float),
        weights=decay_weights,
    )

    nb_params = nb_cov = nb_mu = nb_alpha = None
    nb_success = False
    nb_message = "not fitted"
    if fit_neg_binom:
        nb_params, nb_cov, nb_mu, nb_alpha, nb_success, nb_message = fit_negative_binomial(
            x, y, init_params=freq_params.to_numpy(dtype=float), weights=decay_weights
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
        nb_params=nb_params,
        nb_cov=nb_cov,
        nb_mu=nb_mu,
        nb_alpha=nb_alpha,
        nb_success=nb_success,
        nb_message=nb_message,
        decay_weights=decay_weights,
    )


def _team_coef(params: pd.Series, prefix: str, team: str, baseline_team: str) -> float:
    if team == baseline_team:
        return 0.0
    return float(params.get(f"{prefix}_{team}", 0.0))


def league_team_summary(bundle: ModelBundle, mode: str = "frequentist") -> pd.DataFrame:
    params = _get_params(bundle, mode)

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
    params = _get_params(bundle, mode)
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


def estimate_dixon_coles_rho(bundle: ModelBundle, mode: str = "frequentist") -> float:
    """Estimate the Dixon-Coles score-correlation parameter rho.

    Rho adjusts joint probabilities for low-scoring outcomes (0-0, 1-0, 0-1, 1-1).
    Positive rho → low-scoring results more likely than independent Poisson predicts.
    Typical range: [-0.1, 0.1].
    """
    mu = _get_mu(bundle, mode)
    long_df = bundle.long_df.copy()
    long_df["mu"] = mu

    home_rows = (
        long_df[long_df["is_home"] == 1][["game_id", "goals", "mu"]]
        .rename(columns={"goals": "hg", "mu": "lh"})
    )
    away_rows = (
        long_df[long_df["is_home"] == 0][["game_id", "goals", "mu"]]
        .rename(columns={"goals": "ag", "mu": "la"})
    )
    games = home_rows.merge(away_rows, on="game_id").dropna()
    if games.empty:
        return 0.0

    hg = games["hg"].to_numpy(dtype=float)
    ag = games["ag"].to_numpy(dtype=float)
    lh = games["lh"].to_numpy(dtype=float)
    la = games["la"].to_numpy(dtype=float)

    # Bound rho so all four tau corrections remain positive
    upper = min(0.5, float(np.min(1.0 / np.clip(lh * la, 0.01, None))) * 0.95)
    lower = max(-0.5, -float(np.min(1.0 / np.clip(np.maximum(lh, la), 0.01, None))) * 0.95)
    if lower >= upper:
        return 0.0

    def neg_ll(rho: float) -> float:
        tau = np.ones(len(hg))
        m00 = (hg == 0) & (ag == 0)
        m10 = (hg == 1) & (ag == 0)
        m01 = (hg == 0) & (ag == 1)
        m11 = (hg == 1) & (ag == 1)
        tau[m00] = np.clip(1.0 - lh[m00] * la[m00] * rho, 1e-10, None)
        tau[m10] = np.clip(1.0 + la[m10] * rho, 1e-10, None)
        tau[m01] = np.clip(1.0 + lh[m01] * rho, 1e-10, None)
        tau[m11] = np.clip(1.0 - rho, 1e-10, None)
        return -float(np.sum(np.log(tau)))

    res = opt.minimize_scalar(neg_ll, bounds=(lower, upper), method="bounded")
    return float(res.x)


def score_matrix_probs_dc(
    lambda_home: float,
    lambda_away: float,
    rho: float,
    max_goals: int = 10,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Score matrix with Dixon-Coles low-score correction applied.

    The tau correction factor adjusts the (0,0), (1,0), (0,1), (1,1) cells:
      tau(0,0) = 1 - lambda_home * lambda_away * rho
      tau(1,0) = 1 + lambda_away * rho
      tau(0,1) = 1 + lambda_home * rho
      tau(1,1) = 1 - rho
    The matrix is renormalised after correction.
    """
    home_range = np.arange(0, max_goals + 1)
    away_range = np.arange(0, max_goals + 1)
    home_p = st.poisson.pmf(home_range, lambda_home)
    away_p = st.poisson.pmf(away_range, lambda_away)
    mat = np.outer(home_p, away_p)

    # Apply tau corrections to the four low-score cells
    corrections = {
        (0, 0): max(1.0 - lambda_home * lambda_away * rho, 1e-10),
        (1, 0): max(1.0 + lambda_away * rho, 1e-10),
        (0, 1): max(1.0 + lambda_home * rho, 1e-10),
        (1, 1): max(1.0 - rho, 1e-10),
    }
    for (i, j), tau in corrections.items():
        if i < mat.shape[0] and j < mat.shape[1]:
            mat[i, j] *= tau

    total = mat.sum()
    if total > 0:
        mat /= total

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
    mu = _get_mu(bundle, mode)
    params = _get_params(bundle, mode)

    y = bundle.response
    resid = y - mu
    pearson = np.sum(np.square(resid) / np.clip(mu, 1e-8, None))
    dof = max(len(y) - len(params), 1)

    result: dict[str, float] = {
        "n_obs": int(len(y)),
        "n_games": int(len(bundle.games_used)),
        "n_teams": int(len(bundle.teams)),
        "mean_goals": float(np.mean(y)),
        "mean_fitted": float(np.mean(mu)),
        "pearson_overdispersion": float(pearson / dof),
        "home_multiplier": float(math.exp(params["home"])),
        "baseline_goals": float(math.exp(params["intercept"])),
    }
    if mode == "neg_binom" and bundle.nb_alpha is not None:
        result["nb_alpha"] = bundle.nb_alpha
    return result
