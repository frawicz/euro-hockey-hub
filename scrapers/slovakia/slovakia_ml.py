# =============================================================================
# Hockey Expected Goals (xG) Model — Slovakia League
# Data: data/season_1131_matches.csv
#
# NOTE: This dataset only contains 'goal' and 'penalty' event types.
#       Shot-level data is not available, so xG is modeled at the GOAL level
#       using contextual features parsed from raw_block and other columns.
#       The model predicts goal likelihood given game context (score state,
#       time, special teams, etc.) which can be used to evaluate goal quality.
# =============================================================================

import re
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, brier_score_loss
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import LabelEncoder

# =============================================================================
# 1. LOAD DATA
# =============================================================================

df = pd.read_csv("data/season_1131_pbp_events.csv")
print(f"Loaded {len(df):,} events from {df['match_id'].nunique()} matches.")
print(f"Event types found: {df['event_type'].unique()}")
print(f"\nSample raw_block:\n{df['raw_block'].iloc[0]}\n")


# =============================================================================
# 2. PARSE raw_block
#    All structured data lives here. Format examples:
#    GOAL:    "05:11 | 0:1 | HK Poprad | GABOR Ralph Kevin | (1) | SPROUL Ryan..."
#    PENALTY: "06:28 | 2 min. | HK Poprad | NEMČÍK, Martin | (Bitka)"
# =============================================================================

def parse_goal_block(raw):
    """
    Parse a goal raw_block string into structured fields.
    Returns a dict with time, score, team, scorer, assists, plus/minus players.
    """
    result = {
        'rb_time': None,
        'rb_score': None,
        'rb_home_score': None,
        'rb_away_score': None,
        'rb_team': None,
        'rb_scorer': None,
        'rb_assist1': None,
        'rb_assist2': None,
        'rb_plus_count': 0,
        'rb_minus_count': 0,
        'rb_goal_minute': None,
    }
    if not isinstance(raw, str):
        return result

    parts = [p.strip() for p in raw.split('|')]
    if len(parts) < 2:
        return result

    # Time (e.g. "05:11")
    result['rb_time'] = parts[0].strip()
    try:
        m, s = map(int, result['rb_time'].split(':'))
        result['rb_goal_minute'] = m + s / 60.0
    except Exception:
        pass

    # Score (e.g. "0:1")
    score_match = re.search(r'(\d+):(\d+)', parts[1])
    if score_match:
        result['rb_home_score'] = int(score_match.group(1))
        result['rb_away_score'] = int(score_match.group(2))
        result['rb_score'] = f"{score_match.group(1)}-{score_match.group(2)}"

    # Team
    if len(parts) > 2:
        result['rb_team'] = parts[2].strip()

    # Scorer (remove trailing "(N)" goal number)
    if len(parts) > 3:
        scorer_raw = parts[3].strip()
        result['rb_scorer'] = re.sub(r'\s*\(\d+\)\s*$', '', scorer_raw).strip()

    # Assists — look for player names followed by (N) after scorer
    if len(parts) > 4:
        assist_parts = parts[4:]
        assist_names = []
        for ap in assist_parts:
            ap = ap.strip()
            # Stop at plus/minus player lists
            if '+' in ap or '-' in ap or ';' in ap:
                break
            name_match = re.match(r'^([A-ZÁČĎÉÍĽŇÓŠŤÚÝŽ][^(]+)\s*\(\d+\)', ap)
            if name_match:
                assist_names.append(name_match.group(1).strip())
        if len(assist_names) > 0:
            result['rb_assist1'] = assist_names[0]
        if len(assist_names) > 1:
            result['rb_assist2'] = assist_names[1]

    # Plus/minus player counts (ice players at time of goal)
    plus_match = re.search(r'\+\s*([\d,\s]+);', raw)
    if plus_match:
        result['rb_plus_count'] = len([x for x in plus_match.group(1).split(',') if x.strip()])

    minus_match = re.search(r'-\s*([\d,\s]+);', raw)
    if minus_match:
        result['rb_minus_count'] = len([x for x in minus_match.group(1).split(',') if x.strip()])

    return result


def parse_penalty_block(raw):
    """
    Parse a penalty raw_block string.
    Returns penalty minutes and reason.
    """
    result = {'rb_penalty_min': None, 'rb_penalty_reason': None, 'rb_penalty_time': None}
    if not isinstance(raw, str):
        return result

    # Time
    time_match = re.match(r'(\d+:\d+)', raw)
    if time_match:
        result['rb_penalty_time'] = time_match.group(1)

    # Minutes
    min_match = re.search(r'(\d+)\s*min\.', raw)
    if min_match:
        result['rb_penalty_min'] = int(min_match.group(1))

    # Reason (in parentheses at end)
    reason_match = re.search(r'\(([^)]+)\)\s*$', raw)
    if reason_match:
        result['rb_penalty_reason'] = reason_match.group(1).strip()

    return result


# Apply parsers
print("Parsing raw_block fields...")

goals_mask     = df['event_type'].str.lower() == 'goal'
penalties_mask = df['event_type'].str.lower() == 'penalty'

# Parse goals
goal_parsed = df[goals_mask]['raw_block'].apply(parse_goal_block)
goal_df = pd.DataFrame(list(goal_parsed), index=df[goals_mask].index)
df = df.join(goal_df)

# Parse penalties separately for special teams analysis
pen_parsed = df[penalties_mask]['raw_block'].apply(parse_penalty_block)
pen_df = pd.DataFrame(list(pen_parsed), index=df[penalties_mask].index)
df = df.join(pen_df)

print(f"Parsed {goals_mask.sum()} goals and {penalties_mask.sum()} penalties.")


# =============================================================================
# 3. BUILD GOAL-LEVEL DATASET WITH FEATURES
#    Since we have no shot data, each GOAL is one observation.
#    We enrich each goal with game context features.
# =============================================================================

goals = df[goals_mask].copy()

# --- Score state at moment of goal ---
# Subtract 1 because the score in raw_block is AFTER the goal was scored
goals['score_total_before'] = goals['rb_home_score'].fillna(0) + goals['rb_away_score'].fillna(0) - 1
goals['score_diff_before']  = (goals['rb_home_score'].fillna(0) - goals['rb_away_score'].fillna(0))

# Was this goal scored to tie, take the lead, or pile on?
goals['is_tying_goal']    = (goals['score_diff_before'] == 0).astype(int)
goals['is_go_ahead_goal'] = (goals['score_diff_before'].abs() == 1).astype(int)
goals['is_blowout_goal']  = (goals['score_diff_before'].abs() >= 3).astype(int)

# --- Time features from parsed minute ---
goals['goal_minute']     = goals['rb_goal_minute']
goals['is_early_game']   = (goals['goal_minute'] <= 5).astype(int)
goals['is_late_period']  = (goals['goal_minute'] % 20 >= 17).astype(int)  # last 3 min of period
goals['is_overtime_goal'] = (goals['goal_minute'] > 60).astype(int)

# --- Special teams: infer from skater counts on ice ---
# Even-strength = 5 skaters per side; power play = scoring team has more
goals['plus_players']            = goals['rb_plus_count'].fillna(0)
goals['minus_players']           = goals['rb_minus_count'].fillna(0)
goals['skater_advantage']        = goals['plus_players'] - goals['minus_players']
goals['is_powerplay_inferred']   = (goals['skater_advantage'] > 0).astype(int)
goals['is_shorthanded_inferred'] = (goals['skater_advantage'] < 0).astype(int)
goals['is_even_strength']        = (goals['skater_advantage'] == 0).astype(int)

# --- Assist context ---
goals['has_primary_assist']   = goals['rb_assist1'].notna().astype(int)
goals['has_secondary_assist'] = goals['rb_assist2'].notna().astype(int)
goals['num_assists']          = goals['has_primary_assist'] + goals['has_secondary_assist']

print(f"\nGoal dataset: {len(goals)} goals")
print(goals[['goal_minute', 'score_diff_before', 'skater_advantage', 'num_assists']].describe())


# =============================================================================
# 4. PENALTY ENRICHMENT
#    Count penalties per match before each goal to infer special teams volume.
# =============================================================================

penalties = df[penalties_mask][['match_id', 'event_idx', 'rb_penalty_min']].copy()
penalties['rb_penalty_min'] = pd.to_numeric(penalties['rb_penalty_min'], errors='coerce')

match_penalty_totals = penalties.groupby('match_id')['rb_penalty_min'].sum().rename('match_total_pen_min')
goals = goals.join(match_penalty_totals, on='match_id')
goals['match_total_pen_min'] = goals['match_total_pen_min'].fillna(0)

print(f"\nPenalty enrichment done. Avg penalty min per match: {goals['match_total_pen_min'].mean():.1f}")


# =============================================================================
# 5. TEAM ENCODING
# =============================================================================

le = LabelEncoder()
goals['team_encoded'] = le.fit_transform(goals['rb_team'].fillna('unknown'))
print(f"\nTeams found: {list(le.classes_)}")


# =============================================================================
# 6. GOAL CONTEXT MODEL
#    Without shot data we can't do classic xG, but we can model:
#    "Given game context, is this a power play goal?"
#    This classifies goal quality/situation and surfaces the key drivers.
# =============================================================================

FEATURES = [
    'goal_minute',
    'score_diff_before',
    'score_total_before',
    'is_tying_goal',
    'is_go_ahead_goal',
    'is_blowout_goal',
    'is_early_game',
    'is_late_period',
    'is_overtime_goal',
    'plus_players',
    'minus_players',
    'skater_advantage',
    'num_assists',
    'match_total_pen_min',
    'team_encoded',
]

X = goals[FEATURES].fillna(0)
y = goals['is_powerplay_inferred']  # target: power play goal yes/no

print(f"\nPower play goals: {y.sum()} / {len(y)} ({y.mean()*100:.1f}%)")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

base_model = GradientBoostingClassifier(
    n_estimators=200,
    max_depth=3,
    learning_rate=0.05,
    subsample=0.8,
    random_state=42
)

calibrated_model = CalibratedClassifierCV(base_model, cv=5, method='isotonic')
calibrated_model.fit(X_train, y_train)

y_pred = calibrated_model.predict_proba(X_test)[:, 1]

print(f"\n{'='*45}")
print(f"  AUC:         {roc_auc_score(y_test, y_pred):.3f}")
print(f"  Brier score: {brier_score_loss(y_test, y_pred):.4f}")
print(f"{'='*45}")

# Attach goal quality score back to dataset
goals['goal_context_score'] = calibrated_model.predict_proba(X)[:, 1]


# =============================================================================
# 7. FEATURE IMPORTANCE
# =============================================================================

inner_model = calibrated_model.calibrated_classifiers_[0].estimator
importances = pd.Series(inner_model.feature_importances_, index=FEATURES)

print("\n--- Feature Importances (Goal Context Model) ---")
print(importances.sort_values(ascending=False).to_string())


# =============================================================================
# 8. PLAYER ANALYSIS — Scorers & Assisters
# =============================================================================

scorer_stats = (
    goals.groupby('rb_scorer')
    .agg(
        goals          = ('rb_scorer', 'count'),
        pp_goals       = ('is_powerplay_inferred', 'sum'),
        sh_goals       = ('is_shorthanded_inferred', 'sum'),
        es_goals       = ('is_even_strength', 'sum'),
        ot_goals       = ('is_overtime_goal', 'sum'),
        tying_goals    = ('is_tying_goal', 'sum'),
        go_ahead_goals = ('is_go_ahead_goal', 'sum'),
        avg_minute     = ('goal_minute', 'mean'),
    )
    .sort_values('goals', ascending=False)
)

print("\n--- Top 20 Scorers ---")
print(scorer_stats.head(20).to_string())

assist_stats = (
    goals[goals['rb_assist1'].notna()]
    .groupby('rb_assist1')
    .agg(primary_assists=('rb_assist1', 'count'))
    .sort_values('primary_assists', ascending=False)
)

print("\n--- Top 20 Primary Assisters ---")
print(assist_stats.head(20).to_string())


# =============================================================================
# 9. TEAM ANALYSIS
# =============================================================================

team_stats = (
    goals.groupby('rb_team')
    .agg(
        goals_scored    = ('rb_team', 'count'),
        pp_goals        = ('is_powerplay_inferred', 'sum'),
        sh_goals        = ('is_shorthanded_inferred', 'sum'),
        es_goals        = ('is_even_strength', 'sum'),
        ot_goals        = ('is_overtime_goal', 'sum'),
        avg_goal_minute = ('goal_minute', 'mean'),
        avg_score_diff  = ('score_diff_before', 'mean'),
    )
    .assign(pp_rate=lambda d: d['pp_goals'] / d['goals_scored'])
    .sort_values('goals_scored', ascending=False)
)

print("\n--- Team Goal Stats ---")
print(team_stats.to_string())

# Penalty discipline per team
pen_full = df[penalties_mask].copy()
pen_full['rb_penalty_min'] = pd.to_numeric(pen_full['rb_penalty_min'], errors='coerce')

penalty_stats = (
    pen_full.groupby('team')
    .agg(
        total_penalties = ('team', 'count'),
        total_pen_min   = ('rb_penalty_min', 'sum'),
        avg_pen_min     = ('rb_penalty_min', 'mean'),
    )
    .sort_values('total_pen_min', ascending=False)
)

print("\n--- Team Penalty Stats ---")
print(penalty_stats.to_string())


# =============================================================================
# 10. GAME FLOW — Goal timing by 5-minute buckets
# =============================================================================

goals['minute_bucket'] = (goals['goal_minute'] // 5 * 5).astype(int)
timing = goals.groupby('minute_bucket').size().rename('goals')

print("\n--- Goals by 5-Minute Bucket ---")
print(timing.to_string())


# =============================================================================
# 11. SAVE OUTPUTS
# =============================================================================

goals.to_csv("slovakia/data/goals_enriched.csv", index=False)
scorer_stats.to_csv("slovakia/data/scorer_stats.csv")
team_stats.to_csv("slovakia/data/team_stats.csv")
penalty_stats.to_csv("slovakia/data/penalty_stats.csv")

print("\nSaved:")
print("  data/goals_enriched.csv")
print("  data/scorer_stats.csv")
print("  data/team_stats.csv")
print("  data/penalty_stats.csv")
print("\nDone.")