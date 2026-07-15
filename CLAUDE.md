# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python environment
- This project uses a virtual environment at `.venv`.
- Always run Python and pip through the virtual environment directly:
  - `.venv/bin/python`
  - `.venv/bin/python -m pip`
- Prefer running project tools through the venv Python, for example:
  - `.venv/bin/python -m pip install ...`
- Do not assume `source .venv/bin/activate` persists between commands; each shell command may run in a fresh shell.
- If activation is required for a tool, chain it in the same command:
  - `source .venv/bin/activate && <command>`
- If `.venv` does not exist, ask before creating it.
- Before installing or upgrading any package, briefly say what you need and why, then install it. Add new dependencies to requirements.txt (or pyproject.toml).

## Project Overview

A portfolio data pipeline that ingests Chicago Divvy bike-share trip data into Postgres and transforms it into analytics-ready models. The repo intentionally contains **two parallel transformation layers** plus an orchestration layer on top:

1. **Original SQL pipeline** (`sql/`) — the frozen first version, run by hand with `psql`. Fully decoupled from dbt: nothing depends on it.
2. **dbt layer** (`dbt/`) — the analytics-engineering rebuild, sourcing directly from `raw.trips_raw` and owning the full flow: source (raw) → staging (view) → intermediate (incremental) → marts, processed one `source_month` at a time.
3. **Airflow** (`airflow/`) — local orchestration that chains the Python ingestion and the dbt layer into one rerunnable DAG.

The SQL layer is kept in place as a reference/legacy version — don't remove or "fix" it into the dbt layer; they're meant to coexist as independent implementations.

## Data Flow / Architecture

```
ingestion/load_divvy_month.py  (download zip → extract csv → COPY into Postgres)
        ↓
raw.trips_raw                                  (raw landing table, all columns TEXT)
        ↓ source() in dbt/models/sources/sources.yml
analytics_dbt.stg_divvy_trips                  (staging view — source of truth for valid trips)
        ↓ ref()   ← the only scan of raw, filtered to one source_month
analytics_dbt.int_trips                        (incremental, trip grain, typed)
        ↓ ref()                     ↓ ref()
analytics_dbt.mart_trips_daily     analytics_dbt.int_station_activity_monthly
(incremental, daily grain)         (incremental, month × station grain)
                                          ↓ ref()
                                   analytics_dbt.mart_station_activity (all-time rollup table)

Legacy SQL layer (manual-only, fully decoupled — dbt does not read from it):
raw.trips_raw → sql/10_stg_trips.sql → analytics.stg_trips
             → sql/30_mart_trips_daily.sql → analytics.mart_trips_daily
```

Key cleaning rules applied in `dbt/models/staging/stg_divvy_trips.sql` (the source of truth for what counts as a "valid" trip; a faithful port of the legacy `sql/10_stg_trips.sql`):
- `started_at` and `ended_at` must be non-null and `ended_at > started_at`.
- Trip duration must be between 1 minute and 24 hours.
- Rider type is normalized to lowercase `member`/`casual`, else NULL, exposed as `member_casual`.
- `NULLIF(col, '')` is applied before every cast (casting `''` to timestamp errors in Postgres) — preserve this ordering.
- The view must stay projections + row filters only (no aggregation/window/DISTINCT) so Postgres inlines it and pushes `int_trips`' `source_month` filter down to the raw scan.

Incremental pattern: `int_trips`, `int_station_activity_monthly`, and `mart_trips_daily` are `incremental` with `delete+insert` (partition replace per `source_month` / `trip_date`), mirroring ingestion's per-month delete+insert. Because a month's raw file can contain trips that started on the last day of the previous month, `mart_trips_daily` recomputes each affected `trip_date` from all source_months (not just the current one) — otherwise boundary dates would be replaced with partial counts. Incremental runs require `--vars '{"source_month": "YYYYMM"}'`; missing var fails with a clear error. First run / `--full-refresh` rebuilds all months and needs no var. `dbt compile`/`dbt docs generate` also need the var once the incremental tables exist. Grain determines strategy: the all-time `mart_station_activity` (global window pct, all-time min/max) can't be incremental, so it's a plain-table rollup over the monthly intermediate.

The Airflow DAG (`airflow/dags/divvy_pipeline.py`) reruns this flow per `source_month`:
```
load_raw_trips >> dbt_run >> dbt_test >> summarize_outputs
```
- `load_raw_trips` downloads the month's zip and extracts the CSV when they aren't already cached under `data/`, then deletes/replaces rows for that `source_month` only (rerunnable).
- The default `source_month` is `202506` — the latest validated month of the 202407–202506 backfill, updated by hand when a new month is validated.
- `dbt_run` passes `--vars '{"source_month": "<month>"}'`; the incremental models delete+insert that month's partitions, so re-triggering a loaded month is idempotent.
- A boolean `full_refresh` DAG param (default false) adds `--full-refresh` to rebuild the incremental models from scratch.

## Environment Setup

- Python deps: `requirements.txt` (full dev/notebook env) and `airflow/requirements.txt` (minimal, installed inside the Airflow container).
- Activate venv: `source .venv/bin/activate`
- Requires a `.env` file at the project root (see `.env.example`) defining `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DATABASE_URL`. Both the ingestion script and the Airflow DAG load this file.

## Common Commands

### Postgres
```bash
docker compose up -d        # start Postgres (from project root)
docker compose down
```

### Ingestion
```bash
python ingestion/load_divvy_month.py 202407           # YYYYMM; downloads zip, extracts csv, loads into raw.trips_raw
python scripts/backfill_months.py 202407 202506       # inclusive YYYYMM range, oldest first; add --dbt to follow with dbt run --full-refresh + dbt test
```
The backfill script reuses the ingestion module's functions (it forks no logic); it preflights every zip URL and aborts before loading if a CSV header doesn't match `raw.trips_raw` (schema drift, checked in both directions).
Raw column names are slugified from the CSV header, so `raw.trips_raw` schema is derived dynamically — check `ingestion/load_divvy_month.py` if columns ever change.

### SQL layer
```bash
psql "$DATABASE_URL" -f sql/00_init.sql           # creates raw/analytics schemas
psql "$DATABASE_URL" -f sql/10_stg_trips.sql      # rebuilds analytics.stg_trips
psql "$DATABASE_URL" -f sql/30_mart_trips_daily.sql
psql "$DATABASE_URL" -f sql/20_kpis.sql           # ad-hoc KPI queries (member vs casual, by hour/day, top stations, etc.)
```

### dbt layer (from `dbt/`)
```bash
dbt debug
dbt run --full-refresh                            # first run / rebuild all months (no var needed)
dbt run --vars '{"source_month": "202506"}'       # incremental: process one month
dbt test                                          # no var needed
dbt docs generate --vars '{"source_month": "202506"}'   # var needed once incremental tables exist
dbt docs serve
```
- Profile name is `divvy_analytics`, schema `analytics_dbt`. The local profile (typically `~/.dbt/profiles.yml`) uses target `dev` with hardcoded connection values; the Airflow container uses `airflow/dbt_profiles/profiles.yml` with target `local`.
- In the Airflow profile, dbt connection vars come from the same Postgres env vars (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, plus `DBT_POSTGRES_HOST`/`DBT_POSTGRES_PORT`, defaulting to `localhost:5432`); the local profile hardcodes its values.
- Staging models materialize as views, marts as tables (`dbt/dbt_project.yml`).

### Airflow (from `airflow/`)
```bash
docker compose --env-file ../.env up --build -d   # requires Postgres already running (docker compose up -d from root)
docker compose --env-file ../.env exec airflow airflow dags trigger divvy_pipeline --conf '{"source_month":"202506"}'
docker compose --env-file ../.env down
```
- UI at `http://localhost:8080`, login `admin` / `airflow`.
- The Airflow container mounts the whole project at `/opt/divvy-analytics` and runs project commands (psql, dbt, the ingestion script) directly using that checkout — Airflow tasks are thin wrappers around the same commands listed above, not a separate implementation.
- No local CSV is needed before triggering: `load_raw_trips` downloads and extracts the month's file when it isn't already cached under `data/`.

## Notebooks

`notebooks/01_divvy_eda.ipynb` produces the charts in `images/` (rider volume/mix, trip duration trends, station concentration) — regenerate it if those visuals or the underlying marts change.
