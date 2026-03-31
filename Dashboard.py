"""
dashboard.py — entry point / landing page.
Substantive pages live in pages/.
"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils.data import available_leagues, LEAGUES

LEAGUE_URLS = {
    "austria": "https://www.ice.hockey/",
    "czech": "https://www.hokej.cz/",
    "finland": "https://liiga.fi/",
    "germany": "https://www.penny-del.org/",
    "sweden": "https://www.shl.se/",
    "switzerland": "https://www.nationalleague.ch/",
    "slovakia": "https://www.hockeyslovakia.sk/",
}

st.set_page_config(
    page_title="Dashboard",
    page_icon="🏒",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }

[data-testid="stSidebar"] {
    background: #0a0e1a;
    border-right: 1px solid #1e2535;
}
[data-testid="stSidebar"] * { color: #c8d0e0 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stRadio label {
    font-size: 11px !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #5a6480 !important;
    font-weight: 500;
}
[data-testid="stSidebarNav"] ul > li:first-child a {
    text-transform: capitalize !important;
}
[data-testid="metric-container"] {
    background: #0f1624;
    border: 1px solid #1e2535;
    border-radius: 8px;
    padding: 1rem;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace;
    font-size: 1.8rem;
    color: #e8edf8;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #5a6480;
}
[data-testid="stDataFrame"] {
    border: 1px solid #1e2535 !important;
    border-radius: 8px;
}
h1 { font-weight: 600; letter-spacing: -0.02em; color: #e8edf8; }
h2 { font-weight: 500; color: #e8edf8; font-size: 1.1rem; }
h3 { font-weight: 500; color: #8a94b0; font-size: 0.85rem;
     text-transform: uppercase; letter-spacing: 0.08em; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🏒 Euro Hockey Hub")
    st.markdown("---")
    
    active = available_leagues()
    print(active)
    for lg in active:
        cfg = LEAGUES[lg]
        league_url = LEAGUE_URLS.get(lg, "")
        label_html = (
            f"<a href='{league_url}' target='_blank' rel='noopener noreferrer' "
            f"title='{league_url}' style='color:#5a6480;font-size:12px;text-decoration:none'>"
            f"{cfg['label']}</a>"
            if league_url
            else f"<span style='color:#5a6480;font-size:12px'>{cfg['label']}</span>"
        )
        st.markdown(
            f"<span style='color:{cfg['color']};font-size:12px;font-weight:600;"
            f"font-family:DM Mono,monospace'>{cfg['abbr']}</span>"
            f"<span style='color:#5a6480;font-size:12px'> </span>{label_html}",
            unsafe_allow_html=True,
        )
    st.markdown("---")

st.title("Dashboard")
st.markdown(
    "<p style='color:#5a6480;font-size:1.1rem;margin-top:-0.5rem'>"
    "Euro Hockey Hub · Multi-league European ice hockey analytics</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

if not active:
    st.warning(
        "No data found. Run your scrapers and place the CSVs in "
        "each league's `data/input/` folder."
    )
else:
    cols = st.columns(len(active))
    for col, lg in zip(cols, active):
        cfg = LEAGUES[lg]
        with col:
            st.markdown(
                f"<div style='background:#0f1624;border:1px solid #1e2535;"
                f"border-left:3px solid {cfg['color']};border-radius:8px;"
                f"padding:1rem;text-align:center'>"
                f"<div style='font-family:DM Mono,monospace;font-size:1.4rem;"
                f"font-weight:500;color:{cfg['color']}'>{cfg['abbr']}</div>"
                f"<div style='font-size:0.8rem;color:#5a6480;margin-top:4px'>"
                f"{cfg['label']}</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown("""
    <br>
    <p style='color:#5a6480'>Use the sidebar to navigate between pages:</p>
    <ul style='color:#8a94b0;line-height:2'>
      <li><b>Overview</b> — top metrics, recent results, goals per league</li>
      <li><b>Game Results</b> — searchable results table + score distribution</li>
      <li><b>Team Stats</b> — team performance table, PP%, PIM/G, first-goal impact</li>
      <li><b>Cross-League</b> — compare leagues on scoring, penalties, nationality</li>
      <li><b>Home Advantage</b> — Bayesian home-ice estimates with user prior controls</li>
      <li><b>Goal-State Matrix</b> — next-goal score/concede probabilities by team and goal difference</li>
      <li><b>Data & Methodology</b> — sources, scraping pipeline, and metric definitions</li>
    </ul>
    """, unsafe_allow_html=True)
