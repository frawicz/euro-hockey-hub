"""
Odds vs Results Analysis — market calibration, Brier score, log loss, and ROI.

Requires scrapers/odds/data/input/oddsportal_games.csv to exist.
Run the scraper first:
    python scrapers/odds/oddsportal_scraper.py --mode results --pages 3
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data import LEAGUES, PLOTLY_LAYOUT, build_sidebar, load_oddsportal_data, oddsportal_file_exists

st.set_page_config(page_title="Odds Analysis · Euro Hockey Hub", page_icon="📐", layout="wide")
sel_league_keys, _sel_season, _sel_phase = build_sidebar()

st.title("Odds vs Results Analysis")
st.caption("Calibration · Brier score · Log loss · ROI · Market efficiency")

if not oddsportal_file_exists():
    st.warning("No OddsPortal data found.")
    st.code("python scrapers/odds/oddsportal_scraper.py --mode results --pages 3", language="bash")
    st.stop()

raw = load_oddsportal_data()
if raw.empty:
    st.error("Could not load odds data.")
    st.stop()


# ── Build analysis dataset ────────────────────────────────────────────────────
# OddsPortal 1X2 reflects the regulation result:
#   status = "final"             → decided in regulation
#   status = "final_ot/final_so" → tied at regulation end (= draw in 1X2 market)
#
# For each game we know: home implied prob, draw implied prob, away implied prob,
# and the actual regulation outcome (home win / draw / away win).
# We need all three outcomes present to compute vig-removed probs.

COMPLETED = {"final", "final_ot", "final_so"}

def build_game_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot to one row per game with home/draw/away implied probs and actual outcome.
    """
    completed = df[df["status"].isin(COMPLETED)].copy()
    if completed.empty:
        return pd.DataFrame()

    completed["european_odds"] = pd.to_numeric(completed["european_odds"], errors="coerce")
    completed["home_score"]    = pd.to_numeric(completed["home_score"],    errors="coerce")
    completed["away_score"]    = pd.to_numeric(completed["away_score"],    errors="coerce")
    completed = completed.dropna(subset=["european_odds", "home_score", "away_score"])

    # Pivot to wide: one row per game
    wide = completed.pivot_table(
        index=["game_url", "league", "league_label", "match_date",
               "home_team", "away_team", "home_score", "away_score", "status"],
        columns="outcome_type",
        values="european_odds",
        aggfunc="mean",
    ).reset_index()
    wide.columns.name = None

    # Need at least home + away
    if "home" not in wide.columns or "away" not in wide.columns:
        return pd.DataFrame()

    wide["home_raw"]  = 1.0 / wide["home"]
    wide["away_raw"]  = 1.0 / wide["away"]
    wide["draw_raw"]  = 1.0 / wide["draw"] if "draw" in wide.columns else 0.0

    total = wide["home_raw"] + wide["away_raw"] + wide["draw_raw"]
    wide["vig"]        = total - 1.0
    wide["home_prob"]  = wide["home_raw"]  / total
    wide["away_prob"]  = wide["away_raw"]  / total
    wide["draw_prob"]  = wide["draw_raw"]  / total if "draw" in wide.columns else 0.0

    # Actual regulation outcome
    # final_ot / final_so → draw at regulation end
    is_draw = wide["status"].isin({"final_ot", "final_so"})
    is_home = (~is_draw) & (wide["home_score"] > wide["away_score"])
    is_away = (~is_draw) & (wide["away_score"] > wide["home_score"])

    wide["actual_home"] = is_home.astype(int)
    wide["actual_draw"] = is_draw.astype(int)
    wide["actual_away"] = is_away.astype(int)

    # Drop rows where outcome is ambiguous
    valid = is_home | is_draw | is_away
    return wide[valid].reset_index(drop=True)


games = build_game_table(raw)

if games.empty:
    st.info("Not enough completed games with odds to run analysis. Check that home/draw/away odds and scores are populated.")
    st.stop()


# ── Brier Score & Log Loss helpers ────────────────────────────────────────────

def brier(prob: pd.Series, actual: pd.Series) -> float:
    return float(((prob - actual) ** 2).mean())


def log_loss_safe(prob: pd.Series, actual: pd.Series, eps: float = 1e-7) -> float:
    p = prob.clip(eps, 1 - eps)
    return float(-(actual * np.log(p) + (1 - actual) * np.log(1 - p)).mean())


def roi(odds: pd.Series, actual: pd.Series) -> float:
    """Flat-bet ROI: bet 1 unit on every game, return = odds if win else 0."""
    returns = np.where(actual == 1, odds, 0.0)
    return float((returns.mean() - 1.0))  # as a fraction


# ── Date filter (appended to shared sidebar) ─────────────────────────────────

sel_labels = [LEAGUES[lg]["label"] for lg in sel_league_keys if lg in LEAGUES]

min_d = games["match_date"].min()
max_d = games["match_date"].max()
with st.sidebar:
    st.markdown("### Date Range")
    sel_dates = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
if isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
    date_from, date_to = pd.Timestamp(sel_dates[0]), pd.Timestamp(sel_dates[1])
else:
    date_from, date_to = min_d, max_d

mask = (
    games["league_label"].isin(sel_labels) &
    games["match_date"].between(date_from, date_to)
)
g = games[mask].copy()

if g.empty:
    st.info("No data matches the current filters.")
    st.stop()


# ── Top-level metrics ─────────────────────────────────────────────────────────

n_games   = len(g)
avg_vig   = g["vig"].mean()
bs_home   = brier(g["home_prob"], g["actual_home"])
bs_draw   = brier(g["draw_prob"], g["actual_draw"])
bs_away   = brier(g["away_prob"], g["actual_away"])
bs_avg    = (bs_home + bs_draw + bs_away) / 3
ll_home   = log_loss_safe(g["home_prob"], g["actual_home"])

home_win_rate    = g["actual_home"].mean()
avg_implied_home = g["home_prob"].mean()

st.markdown("### Summary")
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Games analysed", f"{n_games:,}")
with c2:
    st.metric("Avg vig", f"{avg_vig * 100:.2f}%")
with c3:
    st.metric("Brier score (home)", f"{bs_home:.4f}", help="Lower = better. 0 = perfect. Random = 0.25.")
with c4:
    st.metric("Log loss (home)", f"{ll_home:.4f}", help="Lower = better. Heavily penalises confident errors.")
with c5:
    delta = home_win_rate - avg_implied_home
    st.metric(
        "Actual vs implied home win%",
        f"{home_win_rate * 100:.1f}% vs {avg_implied_home * 100:.1f}%",
        delta=f"{delta * 100:+.1f}pp",
        help="Positive = market underestimates home advantage.",
    )


# ── Per-league metrics table ──────────────────────────────────────────────────

st.markdown("### Per-League Metrics")
st.caption(
    "Brier score: lower = better. 0.25 = random guessing. "
    "ROI: return per €1 flat-bet (negative = lose money long-term, as expected). "
    "Home bias = actual home win rate minus market-implied home win rate."
)

league_rows = []
for lg, grp in g.groupby("league_label"):
    league_rows.append({
        "League":          lg,
        "Games":           len(grp),
        "Avg vig %":       round(grp["vig"].mean() * 100, 2),
        "Home win %":      round(grp["actual_home"].mean() * 100, 1),
        "Implied home %":  round(grp["home_prob"].mean() * 100, 1),
        "Home bias pp":    round((grp["actual_home"].mean() - grp["home_prob"].mean()) * 100, 1),
        "Brier (home)":    round(brier(grp["home_prob"], grp["actual_home"]), 4),
        "Log loss (home)": round(log_loss_safe(grp["home_prob"], grp["actual_home"]), 4),
        "ROI home":        f"{roi(grp['home'], grp['actual_home']) * 100:+.1f}%",
        "ROI draw":        f"{roi(grp['draw'], grp['actual_draw']) * 100:+.1f}%" if "draw" in grp.columns else "–",
        "ROI away":        f"{roi(grp['away'], grp['actual_away']) * 100:+.1f}%",
    })

league_df = pd.DataFrame(league_rows).sort_values("League")
st.dataframe(league_df, hide_index=True, use_container_width=True)


# ── Calibration curve ─────────────────────────────────────────────────────────

st.markdown("### Calibration Curves")
st.caption(
    "Games bucketed into 5% probability bins. Each point compares the market's "
    "predicted probability (x) to the actual win rate in that bucket (y). "
    "A perfectly calibrated market follows the diagonal."
)

tab_home, tab_draw, tab_away = st.tabs(["Home win", "Draw (regulation)", "Away win"])

def calibration_fig(prob_col: str, actual_col: str, label: str) -> go.Figure:
    fig = go.Figure()
    # Perfect calibration diagonal
    fig.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1], mode="lines",
        line=dict(color="#3a4460", dash="dash", width=1),
        name="Perfect calibration", showlegend=True,
    ))
    colors = px.colors.qualitative.Plotly
    for i, (lg, grp) in enumerate(g.groupby("league_label")):
        if lg not in sel_labels:
            continue
        grp = grp.dropna(subset=[prob_col])
        grp["bucket"] = (grp[prob_col] * 20).round(0) / 20  # 5% bins
        cal = (
            grp.groupby("bucket")
            .agg(mean_prob=(prob_col, "mean"), actual_rate=(actual_col, "mean"), n=(actual_col, "count"))
            .reset_index()
        )
        cal = cal[cal["n"] >= 3]
        if cal.empty:
            continue
        fig.add_trace(go.Scatter(
            x=cal["mean_prob"], y=cal["actual_rate"],
            mode="markers+lines", name=lg,
            marker=dict(size=cal["n"].clip(upper=40) * 0.5 + 5, color=colors[i % len(colors)]),
            line=dict(color=colors[i % len(colors)], width=1.5),
            customdata=cal[["n"]].values,
            hovertemplate=f"<b>{lg}</b><br>Implied: %{{x:.1%}}<br>Actual: %{{y:.1%}}<br>Games: %{{customdata[0]}}<extra></extra>",
        ))
    fig.update_layout(
        **{**PLOTLY_LAYOUT,
           "xaxis": dict(title=f"Implied {label} probability (vig-removed)", tickformat=".0%", gridcolor="#1e2535"),
           "yaxis": dict(title=f"Actual {label} rate", tickformat=".0%", gridcolor="#1e2535")},
        height=420,
    )
    return fig

with tab_home:
    st.plotly_chart(calibration_fig("home_prob", "actual_home", "home win"), use_container_width=True)
with tab_draw:
    if "draw_prob" in g.columns and g["draw_prob"].gt(0).any():
        st.plotly_chart(calibration_fig("draw_prob", "actual_draw", "draw"), use_container_width=True)
    else:
        st.info("No draw odds available in the current data.")
with tab_away:
    st.plotly_chart(calibration_fig("away_prob", "actual_away", "away win"), use_container_width=True)


# ── ROI by probability bucket ─────────────────────────────────────────────────

st.markdown("### ROI by Implied Probability Bucket")
st.caption(
    "Flat-bet ROI broken down by the market's pre-game implied probability for the home team. "
    "If the market is efficient, all buckets should be near −vig. "
    "Bars significantly above zero suggest systematic mispricing."
)

g["home_bucket"] = pd.cut(
    g["home_prob"],
    bins=[0, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0],
    labels=["<30%", "30–40%", "40–50%", "50–60%", "60–70%", "70–80%", ">80%"],
)

roi_buckets = (
    g.groupby("home_bucket", observed=True)
    .apply(lambda grp: pd.Series({
        "games": len(grp),
        "actual_home_rate": grp["actual_home"].mean() * 100,
        "roi_home": roi(grp["home"], grp["actual_home"]) * 100,
    }))
    .reset_index()
)

fig_roi = go.Figure()
colors_roi = ["#ef5350" if v < 0 else "#66bb6a" for v in roi_buckets["roi_home"]]
fig_roi.add_trace(go.Bar(
    x=roi_buckets["home_bucket"].astype(str),
    y=roi_buckets["roi_home"],
    marker_color=colors_roi,
    text=roi_buckets["roi_home"].round(1).astype(str) + "%",
    textposition="outside",
    customdata=roi_buckets[["games", "actual_home_rate"]].values,
    hovertemplate=(
        "<b>%{x}</b><br>"
        "ROI: %{y:.1f}%<br>"
        "Games: %{customdata[0]}<br>"
        "Actual home win rate: %{customdata[1]:.1f}%<extra></extra>"
    ),
))
fig_roi.add_hline(y=0, line_color="#3a4460", line_dash="dash", line_width=1)
fig_roi.update_layout(
    **{**PLOTLY_LAYOUT,
       "xaxis": dict(title="Implied home win probability", gridcolor="#1e2535"),
       "yaxis": dict(title="ROI (%)", gridcolor="#1e2535")},
    height=360,
)
st.plotly_chart(fig_roi, use_container_width=True)


# ── Home advantage: implied vs actual ────────────────────────────────────────

st.markdown("### Home Advantage: Market vs Reality")
st.caption(
    "Each bar shows how much the actual home win rate differs from the market's implied probability. "
    "Positive (green) = market underestimates home advantage. "
    "Negative (red) = market overestimates home advantage."
)

ha_rows = []
for lg, grp in g.groupby("league_label"):
    ha_rows.append({
        "league": lg,
        "actual": grp["actual_home"].mean() * 100,
        "implied": grp["home_prob"].mean() * 100,
        "bias": (grp["actual_home"].mean() - grp["home_prob"].mean()) * 100,
        "n": len(grp),
    })
ha_df = pd.DataFrame(ha_rows).sort_values("bias", ascending=False)

fig_ha = go.Figure()
fig_ha.add_trace(go.Bar(
    x=ha_df["league"],
    y=ha_df["bias"],
    marker_color=["#66bb6a" if v >= 0 else "#ef5350" for v in ha_df["bias"]],
    text=ha_df["bias"].round(1).astype(str) + " pp",
    textposition="outside",
    customdata=ha_df[["actual", "implied", "n"]].values,
    hovertemplate=(
        "<b>%{x}</b><br>"
        "Bias: %{y:.1f} pp<br>"
        "Actual home win: %{customdata[0]:.1f}%<br>"
        "Implied home win: %{customdata[1]:.1f}%<br>"
        "Games: %{customdata[2]}<extra></extra>"
    ),
))
fig_ha.add_hline(y=0, line_color="#3a4460", line_dash="dash", line_width=1)
fig_ha.update_layout(
    **{**PLOTLY_LAYOUT,
       "xaxis": dict(title="", gridcolor="#1e2535"),
       "yaxis": dict(title="Actual − implied home win % (pp)", gridcolor="#1e2535")},
    height=360,
)
st.plotly_chart(fig_ha, use_container_width=True)


# ── Vig distribution ──────────────────────────────────────────────────────────

st.markdown("### Vig Distribution by League")
st.caption("The bookmaker's built-in edge. Lower = tighter market = fairer odds for bettors.")

fig_vig = px.box(
    g,
    x="league_label",
    y=g["vig"] * 100,
    template="plotly_dark",
    labels={"x": "League", "y": "Vig (%)"},
    color="league_label",
    color_discrete_sequence=px.colors.qualitative.Plotly,
)
fig_vig.update_layout(
    **{**PLOTLY_LAYOUT,
       "xaxis": dict(title="", gridcolor="#1e2535"),
       "yaxis": dict(title="Vig (%)", gridcolor="#1e2535")},
    showlegend=False,
    height=360,
)
st.plotly_chart(fig_vig, use_container_width=True)


# ── Raw game table ────────────────────────────────────────────────────────────

with st.expander("Full game table", expanded=False):
    disp = g[[
        "match_date", "league_label", "home_team", "away_team",
        "home_score", "away_score", "status",
        "home", "draw", "away",
        "home_prob", "draw_prob", "away_prob",
        "actual_home", "actual_draw", "actual_away",
        "vig",
    ]].copy()
    disp["match_date"] = disp["match_date"].dt.strftime("%Y-%m-%d")
    for col in ["home_prob", "draw_prob", "away_prob", "vig"]:
        disp[col] = (disp[col] * 100).round(1)
    st.dataframe(
        disp.rename(columns={
            "match_date": "Date", "league_label": "League",
            "home_team": "Home", "away_team": "Away",
            "home_score": "H", "away_score": "A", "status": "Status",
            "home": "Home odds", "draw": "Draw odds", "away": "Away odds",
            "home_prob": "Home prob%", "draw_prob": "Draw prob%", "away_prob": "Away prob%",
            "actual_home": "H win", "actual_draw": "Draw", "actual_away": "A win",
            "vig": "Vig%",
        }),
        hide_index=True,
        use_container_width=True,
    )
