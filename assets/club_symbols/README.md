# Club Logos Manual Storage

Use this folder for all team logos used by the dashboard.

## Folder structure

- `assets/club_symbols/austria/`
- `assets/club_symbols/czech/`
- `assets/club_symbols/finland/`
- `assets/club_symbols/germany/`
- `assets/club_symbols/slovakia/`
- `assets/club_symbols/sweden/`
- `assets/club_symbols/switzerland/`

## Mapping file

Use: `data/team_symbols_manual.csv`

Columns:
- `league`: league key
- `team`: exact team name from data
- `logo_path`: path to the logo file to use
- `status`: `ready` or `missing`
- `notes`: optional comments

## How to add missing logos

1. Open `data/team_symbols_manual.csv`.
2. Filter rows where `status == missing`.
3. Save each logo file in the league folder indicated by `league`.
4. Keep filename equal to the slug in `logo_path` (or update `logo_path` if you use another name).
5. Set `status` to `ready`.

## Recommended file formats

Prefer `png` (best compatibility), `svg`, or `jpg`.
