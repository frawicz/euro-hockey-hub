"""Compute team Elo ratings for one league or all leagues.

Usage examples:
    python utils/elo_calc.py --league czech
    python utils/elo_calc.py --league all

Outputs (per league):
    scrapers/<league>/data/output/elo_game_log.csv
    scrapers/<league>/data/output/elo_by_round.csv
    scrapers/<league>/data/output/elo_latest.csv
"""

from __future__ import annotations

import argparse
import math
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


def to_num(value):
    return pd.to_numeric(value, errors="coerce")


def to_bool(value) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    txt = str(value).strip().lower()
    return txt in {"1", "true", "yes", "y", "t"}


def parse_score_text(value):
    if pd.isna(value):
        return (None, None)
    m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", str(value))
    if not m:
        return (None, None)
    return (float(m.group(1)), float(m.group(2)))


def derive_season(df: pd.DataFrame) -> pd.Series:
    if "season" in df.columns:
        return df["season"].astype(str)
    if "season_year" in df.columns:
        years = to_num(df["season_year"])
        out = []
        for y in years:
            if pd.isna(y):
                out.append("unknown")
                continue
            y = int(y)
            out.append(f"{y-1}-{str(y)[-2:]}")
        return pd.Series(out, index=df.index)
    if "season_id" in df.columns:
        return df["season_id"].astype(str)
    # Fallback: infer season from date using hockey-season boundary (July).
    date_col = None
    for c in ["date", "start_utc", "end_utc"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is not None:
        dt = pd.to_datetime(df[date_col], errors="coerce", utc=True)
        out = []
        for d in dt:
            if pd.isna(d):
                out.append("unknown")
                continue
            y = int(d.year)
            if int(d.month) >= 7:
                out.append(f"{y}-{str(y + 1)[-2:]}")
            else:
                out.append(f"{y - 1}-{str(y)[-2:]}")
        return pd.Series(out, index=df.index)
    return pd.Series(["unknown"] * len(df), index=df.index)


def derive_date(df: pd.DataFrame) -> pd.Series:
    for col in ["date", "start_utc", "end_utc", "created_at"]:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=True)
            if dt.notna().any():
                return dt
    return pd.Series(pd.NaT, index=df.index)


def derive_scores(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    home = away = None

    home_candidates = ["home_score", "home_goals", "home_score_pT"]
    away_candidates = ["away_score", "away_goals", "away_score_pT"]

    for c in home_candidates:
        if c in df.columns:
            home = to_num(df[c])
            break
    for c in away_candidates:
        if c in df.columns:
            away = to_num(df[c])
            break

    if home is None or away is None:
        if "score" in df.columns:
            parsed = df["score"].apply(parse_score_text)
            parsed_home = parsed.apply(lambda x: x[0])
            parsed_away = parsed.apply(lambda x: x[1])
            if home is None:
                home = parsed_home
            if away is None:
                away = parsed_away

    if home is None:
        home = pd.Series([None] * len(df), index=df.index, dtype="float")
    if away is None:
        away = pd.Series([None] * len(df), index=df.index, dtype="float")

    return home, away


def derive_round(df: pd.DataFrame, season: pd.Series, date: pd.Series) -> pd.Series:
    if "round" in df.columns:
        s = df["round"].astype(str).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        if s.notna().any():
            return s.fillna("Unknown")

    if "game_week" in df.columns:
        s = to_num(df["game_week"]).astype("Int64").astype(str)
        s = s.replace({"<NA>": pd.NA})
        if s.notna().any():
            return "GW " + s.fillna("Unknown")

    if "meta_line" in df.columns:
        extracted = df["meta_line"].astype(str).str.extract(r"(\d+)", expand=False)
        if extracted.notna().any():
            return "R " + extracted.fillna("Unknown")

    # Fallback: build synthetic rounds by season/date order.
    fallback = pd.Series(["Unknown"] * len(df), index=df.index, dtype="object")
    tmp = pd.DataFrame({"season": season, "date": date}, index=df.index)
    for s_key, grp_idx in tmp.groupby("season").groups.items():
        g = tmp.loc[grp_idx].copy()
        # Missing dates go to their own terminal bucket.
        g["_date_norm"] = g["date"].dt.tz_convert(None).dt.floor("D")
        g = g.sort_values(["_date_norm"], kind="stable")
        unique_dates = [d for d in g["_date_norm"].dropna().unique()]
        rank_map = {d: i + 1 for i, d in enumerate(unique_dates)}
        for idx, d in g["_date_norm"].items():
            if pd.isna(d):
                fallback.at[idx] = "R Unknown"
            else:
                fallback.at[idx] = f"R {rank_map[d]}"
    return fallback


def derive_ot_so(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    is_ot = pd.Series([False] * len(df), index=df.index)
    is_so = pd.Series([False] * len(df), index=df.index)

    if "is_overtime" in df.columns:
        is_ot = df["is_overtime"].apply(to_bool)
    if "is_shootout" in df.columns:
        is_so = df["is_shootout"].apply(to_bool)

    if "finished_type" in df.columns:
        ft = df["finished_type"].astype(str)
        is_so = is_so | ft.str.contains("WINNING_SHOT", case=False, na=False)
        is_ot = is_ot | ft.str.contains("OVERTIME", case=False, na=False)

    if "periods" in df.columns:
        # Sweden format sometimes has overtime encoded in period counts.
        p = to_num(df["periods"]) 
        is_ot = is_ot | (p > 3)

    return is_ot, is_so


def mov_multiplier(home_score: float, away_score: float, elo_diff_before: float) -> float:
    margin = abs(home_score - away_score)
    if margin <= 1:
        return 1.0
    return math.log(margin + 1.0) * (2.2 / ((abs(elo_diff_before) * 0.001) + 2.2))


def process_league(league: str, k_factor: float, base_elo: float, carryover: float) -> tuple[int, int]:
    in_path = BASE_DIR / "scrapers" / league / "data" / "input" / "games.csv"
    if not in_path.exists():
        print(f"[{league}] skipped: games.csv not found")
        return (0, 0)

    df = pd.read_csv(in_path, low_memory=False)
    if df.empty:
        print(f"[{league}] skipped: no rows")
        return (0, 0)

    home_col = "home_team" if "home_team" in df.columns else None
    away_col = "away_team" if "away_team" in df.columns else None
    if not home_col or not away_col:
        print(f"[{league}] skipped: missing home_team/away_team")
        return (0, 0)

    season = derive_season(df)
    dt = derive_date(df)
    home_score, away_score = derive_scores(df)
    round_label = derive_round(df, season, dt)
    is_ot, is_so = derive_ot_so(df)

    game_id = df["game_id"] if "game_id" in df.columns else pd.Series(range(1, len(df) + 1), index=df.index)

    work = pd.DataFrame(
        {
            "game_id": game_id.astype(str),
            "season": season.astype(str),
            "date": dt,
            "round": round_label.astype(str),
            "home_team": df[home_col].astype(str),
            "away_team": df[away_col].astype(str),
            "home_score": to_num(home_score),
            "away_score": to_num(away_score),
            "is_overtime": is_ot.astype(bool),
            "is_shootout": is_so.astype(bool),
        }
    )

    work = work.dropna(subset=["home_score", "away_score"])
    work = work[work["home_team"].str.strip() != ""]
    work = work[work["away_team"].str.strip() != ""]

    if work.empty:
        print(f"[{league}] skipped: no valid scored games")
        return (0, 0)

    # Deterministic ordering for Elo path.
    work = work.sort_values(["season", "date", "game_id"], na_position="last").reset_index(drop=True)

    ratings: dict[str, float] = {}
    current_season = None

    game_rows = []
    round_rows = []

    round_idx_map: dict[str, int] = {}
    season_round_counter: dict[str, int] = {}

    def snapshot_round(s: str, r: str, r_idx: int):
        for team, elo in sorted(ratings.items()):
            round_rows.append(
                {
                    "season": s,
                    "round": r,
                    "round_index": r_idx,
                    "team": team,
                    "elo": round(elo, 2),
                }
            )

    prev_round_key = None

    for row in work.itertuples(index=False):
        if current_season is None:
            current_season = row.season
        elif row.season != current_season:
            # Close previous round/season snapshot.
            if prev_round_key is not None:
                ps, pr = prev_round_key.split("|||", 1)
                snapshot_round(ps, pr, round_idx_map[prev_round_key])

            # Carry ratings into new season with mean reversion.
            ratings = {t: (carryover * e + (1.0 - carryover) * base_elo) for t, e in ratings.items()}
            current_season = row.season
            prev_round_key = None

        home = row.home_team
        away = row.away_team
        hs = float(row.home_score)
        aws = float(row.away_score)

        r_home_before = ratings.get(home, base_elo)
        r_away_before = ratings.get(away, base_elo)

        # Round boundary handling: when entering a new round, snapshot previous.
        round_key = f"{row.season}|||{row.round}"
        if prev_round_key is None:
            prev_round_key = round_key
        elif round_key != prev_round_key:
            ps, pr = prev_round_key.split("|||", 1)
            snapshot_round(ps, pr, round_idx_map[prev_round_key])
            prev_round_key = round_key

        if round_key not in round_idx_map:
            season_round_counter[row.season] = season_round_counter.get(row.season, 0) + 1
            round_idx_map[round_key] = season_round_counter[row.season]

        expected_home = 1.0 / (1.0 + 10.0 ** ((r_away_before - r_home_before) / 400.0))

        if hs > aws:
            actual_home = 1.0
            winner = home
        elif hs < aws:
            actual_home = 0.0
            winner = away
        else:
            actual_home = 0.5
            winner = "draw"

        # OT/SO can be treated as a softer win than regulation.
        if winner != "draw" and (bool(row.is_overtime) or bool(row.is_shootout)):
            actual_home = 0.75 if winner == home else 0.25

        winner_diff = (r_home_before - r_away_before) if winner == home else (r_away_before - r_home_before)
        mult = mov_multiplier(hs, aws, winner_diff)

        delta = k_factor * mult * (actual_home - expected_home)

        r_home_after = r_home_before + delta
        r_away_after = r_away_before - delta

        ratings[home] = r_home_after
        ratings[away] = r_away_after

        game_rows.append(
            {
                "game_id": row.game_id,
                "season": row.season,
                "date": row.date,
                "round": row.round,
                "round_index": round_idx_map[round_key],
                "home_team": home,
                "away_team": away,
                "home_score": hs,
                "away_score": aws,
                "is_overtime": bool(row.is_overtime),
                "is_shootout": bool(row.is_shootout),
                "home_elo_before": round(r_home_before, 2),
                "away_elo_before": round(r_away_before, 2),
                "expected_home_win": round(expected_home, 4),
                "actual_home_score": round(actual_home, 2),
                "elo_delta_home": round(delta, 3),
                "home_elo_after": round(r_home_after, 2),
                "away_elo_after": round(r_away_after, 2),
            }
        )

    # Final round snapshot.
    if prev_round_key is not None:
        ps, pr = prev_round_key.split("|||", 1)
        snapshot_round(ps, pr, round_idx_map[prev_round_key])

    games_out = pd.DataFrame(game_rows)
    rounds_out = pd.DataFrame(round_rows)

    latest_out = (
        rounds_out.sort_values(["season", "round_index"]) 
        .groupby("team", as_index=False)
        .tail(1)
        .sort_values("elo", ascending=False)
        .reset_index(drop=True)
    )

    out_dir = BASE_DIR / "scrapers" / league / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    games_out.to_csv(out_dir / "elo_game_log.csv", index=False)
    rounds_out.to_csv(out_dir / "elo_by_round.csv", index=False)
    latest_out.to_csv(out_dir / "elo_latest.csv", index=False)

    print(
        f"[{league}] done: {len(games_out):,} games | "
        f"{len(rounds_out):,} round-team rows | {latest_out['team'].nunique():,} teams"
    )

    return (len(games_out), latest_out["team"].nunique())


def parse_args():
    parser = argparse.ArgumentParser(description="Calculate Elo ratings for hockey leagues.")
    parser.add_argument(
        "--league",
        nargs="+",
        default=["all"],
        help="League key(s): austria czech finland germany slovakia sweden switzerland or all",
    )
    parser.add_argument("--k-factor", type=float, default=20.0, help="Base Elo K-factor")
    parser.add_argument("--base-elo", type=float, default=1500.0, help="Initial Elo for new teams")
    parser.add_argument(
        "--carryover",
        type=float,
        default=0.75,
        help="Season-to-season Elo carryover (0-1). 0.75 keeps 75%% of prior Elo.",
    )
    return parser.parse_args()


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

    total_games = 0
    total_teams = 0
    for lg in targets:
        games, teams = process_league(
            league=lg,
            k_factor=args.k_factor,
            base_elo=args.base_elo,
            carryover=args.carryover,
        )
        total_games += games
        total_teams += teams

    print(f"Finished {len(targets)} league(s) | games processed: {total_games:,} | teams: {total_teams:,}")


if __name__ == "__main__":
    main()
