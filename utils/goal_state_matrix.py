"""Build goal-difference state transition probabilities from play-by-play events.

For each league, outputs:
    scrapers/<league>/data/output/goal_state_team_matrix.csv
    scrapers/<league>/data/output/goal_state_league_matrix.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

LEAGUES = [
    "austria",
    "czech",
    "finland",
    "germany",
    "slovakia",
    "sweden",
    "switzerland",
]

BASE_DIR = Path(__file__).resolve().parent.parent


def norm_token(value: str) -> str:
    txt = str(value or "").upper().strip()
    txt = re.sub(r"[^A-Z0-9]", "", txt)
    return txt


def parse_score_pair(value: object) -> tuple[int, int] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value)
    m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    # Germany fallback in raw_event: trailing "2 3"
    m2 = re.search(r"(\d+)\s+(\d+)\s*$", s)
    if m2:
        return (int(m2.group(1)), int(m2.group(2)))
    return None


def parse_time_sec(value: object) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).strip()
    m = re.match(r"^(\d+):(\d+)$", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return 0.0


def parse_period(value: object) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 1
    s = str(value).upper()
    if s.startswith("P") and len(s) > 1 and s[1:].isdigit():
        return int(s[1:])
    m = re.search(r"(\d+)", s)
    if m:
        return int(m.group(1))
    if "OT" in s:
        return 4
    return 1


def choose_id_col(df: pd.DataFrame) -> str | None:
    for c in ["game_id", "match_id", "game_slug"]:
        if c in df.columns:
            return c
    return None


def build_game_lookup(games: pd.DataFrame) -> tuple[str | None, dict[str, dict]]:
    id_col = choose_id_col(games)
    if id_col is None:
        return (None, {})

    lookup = {}
    for row in games.itertuples(index=False):
        gid = str(getattr(row, id_col))
        home_team = getattr(row, "home_team", None)
        away_team = getattr(row, "away_team", None)
        if home_team is None or away_team is None:
            continue

        home_tokens = {norm_token(home_team)}
        away_tokens = {norm_token(away_team)}

        for hc, ac in [("home_abbr", "away_abbr"), ("home_team_short", "away_team_short"), ("home_slug", "away_slug")]:
            hv = getattr(row, hc, None)
            av = getattr(row, ac, None)
            if hv is not None and not (isinstance(hv, float) and pd.isna(hv)):
                home_tokens.add(norm_token(hv))
            if av is not None and not (isinstance(av, float) and pd.isna(av)):
                away_tokens.add(norm_token(av))

        lookup[gid] = {
            "home_team": str(home_team),
            "away_team": str(away_team),
            "home_tokens": home_tokens,
            "away_tokens": away_tokens,
        }
    return (id_col, lookup)


def infer_side_from_team(team_value: object, game_meta: dict) -> str | None:
    t = norm_token(team_value)
    if not t:
        return None
    if t in game_meta["home_tokens"]:
        return "home"
    if t in game_meta["away_tokens"]:
        return "away"
    return None


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
        p = out["period"].apply(parse_period) if "period" in out.columns else 1
        t = out["time"].apply(parse_time_sec) if "time" in out.columns else 0
        out["_ord"] = p * 100000 + t

    out["_ord"] = out["_ord"].fillna(0)
    out = out.sort_values(["game_key", "_ord"], kind="stable").reset_index(drop=True)
    return out


def counts_to_df(rows: list[dict], level_cols: list[str]) -> pd.DataFrame:
    if not rows:
        cols = level_cols + ["state_diff", "n_total", "n_score", "n_concede", "p_score", "p_concede", "next_if_score", "next_if_concede"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)
    gcols = level_cols + ["state_diff"]
    agg = df.groupby(gcols, as_index=False).agg(n_score=("is_score", "sum"), n_total=("is_score", "count"))
    agg["n_concede"] = agg["n_total"] - agg["n_score"]
    agg["p_score"] = (agg["n_score"] / agg["n_total"]).round(4)
    agg["p_concede"] = (agg["n_concede"] / agg["n_total"]).round(4)
    agg["next_if_score"] = agg["state_diff"] + 1
    agg["next_if_concede"] = agg["state_diff"] - 1
    return agg.sort_values(gcols).reset_index(drop=True)


def process_league(league: str) -> tuple[int, int]:
    games_path = BASE_DIR / "scrapers" / league / "data" / "input" / "games.csv"
    events_path = BASE_DIR / "scrapers" / league / "data" / "input" / "events.csv"
    if not games_path.exists() or not events_path.exists():
        print(f"[{league}] skipped: missing games/events")
        return (0, 0)

    games = pd.read_csv(games_path, low_memory=False)
    events = pd.read_csv(events_path, low_memory=False)
    if games.empty or events.empty:
        print(f"[{league}] skipped: empty games/events")
        return (0, 0)

    _, game_lookup = build_game_lookup(games)
    goal_events = prepare_goal_events(events)
    if goal_events.empty:
        print(f"[{league}] skipped: no goal events")
        return (0, 0)

    team_rows: list[dict] = []
    league_rows: list[dict] = []

    for gid, grp in goal_events.groupby("game_key", sort=False):
        meta = game_lookup.get(str(gid))
        if not meta:
            continue

        home_team = meta["home_team"]
        away_team = meta["away_team"]

        prev_h, prev_a = 0, 0

        for row in grp.itertuples(index=False):
            side = None
            score_after = None

            if hasattr(row, "home_score") and hasattr(row, "away_score"):
                hs = pd.to_numeric(getattr(row, "home_score"), errors="coerce")
                aws = pd.to_numeric(getattr(row, "away_score"), errors="coerce")
                if pd.notna(hs) and pd.notna(aws):
                    score_after = (int(hs), int(aws))

            if score_after is None and hasattr(row, "score_state"):
                score_after = parse_score_pair(getattr(row, "score_state"))

            if score_after is None and hasattr(row, "raw_event"):
                score_after = parse_score_pair(getattr(row, "raw_event"))

            pre_h, pre_a = prev_h, prev_a

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

            if side is None and hasattr(row, "team"):
                side = infer_side_from_team(getattr(row, "team"), meta)

            if side is None and hasattr(row, "team_side"):
                ts = str(getattr(row, "team_side")).strip().lower()
                if ts in {"home", "away"}:
                    side = ts

            if side is None:
                continue

            home_state = pre_h - pre_a
            away_state = -home_state

            team_rows.append({"league": league, "team": home_team, "state_diff": home_state, "is_score": 1 if side == "home" else 0})
            team_rows.append({"league": league, "team": away_team, "state_diff": away_state, "is_score": 1 if side == "away" else 0})

            league_rows.append({"league": league, "state_diff": home_state, "is_score": 1 if side == "home" else 0})
            league_rows.append({"league": league, "state_diff": away_state, "is_score": 1 if side == "away" else 0})

            if score_after is not None:
                prev_h, prev_a = score_after
            elif side == "home":
                prev_h, prev_a = pre_h + 1, pre_a
            else:
                prev_h, prev_a = pre_h, pre_a + 1

    team_df = counts_to_df(team_rows, ["league", "team"])
    lg_df = counts_to_df(league_rows, ["league"])

    out_dir = BASE_DIR / "scrapers" / league / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    team_df.to_csv(out_dir / "goal_state_team_matrix.csv", index=False)
    lg_df.to_csv(out_dir / "goal_state_league_matrix.csv", index=False)

    print(f"[{league}] done: {len(team_df):,} team-state rows | {len(lg_df):,} league-state rows")
    return (len(team_df), len(lg_df))


def parse_args():
    p = argparse.ArgumentParser(description="Build goal-state transition matrices from events")
    p.add_argument("--league", nargs="+", default=["all"], help="league key(s) or all")
    return p.parse_args()


def main():
    args = parse_args()
    req = [x.lower() for x in args.league]
    if "all" in req:
        targets = LEAGUES
    else:
        bad = [x for x in req if x not in LEAGUES]
        if bad:
            raise SystemExit(f"Unknown league(s): {', '.join(bad)}")
        targets = req

    total_team = 0
    total_lg = 0
    for lg in targets:
        t, l = process_league(lg)
        total_team += t
        total_lg += l

    print(f"Finished {len(targets)} league(s) | team rows: {total_team:,} | league rows: {total_lg:,}")


if __name__ == "__main__":
    main()

