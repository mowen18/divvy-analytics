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

1. **Original SQL pipeline** (`sql/`) — the first version, run by hand with `psql`.
2. **dbt layer** (`dbt/`) — a newer analytics-engineering rebuild with model dependencies, tests, and docs, built on top of the SQL layer's output.
3. **Airflow** (`airflow/`) — local orchestration that chains the Python ingestion, SQL layer, and dbt layer together into one rerunnable DAG.

The SQL layer is kept in place as a reference/legacy version — don't remove or "fix" it into the dbt layer; they're meant to coexist.

## Data Flow / Architecture

```
ingestion/load_divvy_month.py  (download zip → extract csv → COPY into Postgres)
        ↓
raw.trips_raw                                  (raw landing table, all columns TEXT)
        ↓ sql/10_stg_trips.sql
analytics.stg_trips                            (cleaned, typed staging table — SQL layer)
        ↓ source() in dbt/models/sources/sources.yml
analytics_dbt.stg_divvy_trips                  (dbt staging view, renames rider_type → member_casual)
        ↓ ref()
analytics_dbt.mart_trips_daily                 (daily trip metrics)
analytics_dbt.mart_station_activity            (station-level metrics)
        ↓ sql/30_mart_trips_daily.sql (SQL-layer equivalent, separate from dbt marts)
analytics.mart_trips_daily
```

Key cleaning rules applied in `sql/10_stg_trips.sql` (the source of truth for what counts as a "valid" trip):
- `started_at` and `ended_at` must be non-null and `ended_at > started_at`.
- Trip duration must be between 1 minute and 24 hours.
- `rider_type` is normalized to lowercase `member`/`casual`, else NULL.

The Airflow DAG (`airflow/dags/divvy_pipeline.py`) reruns this whole flow per `source_month`:
```
check_raw_files >> load_raw_trips >> reset_dbt_schema >> run_sql_staging >> dbt_run >> dbt_test >> summarize_outputs
```
- `load_raw_trips` deletes/replaces rows for that `source_month` only (rerunnable).
- `reset_dbt_schema` drops the entire `analytics_dbt` schema before `dbt run` rebuilds it — dbt models are always fully rebuilt, not incremental.

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
python ingestion/load_divvy_month.py 202401   # YYYYMM; downloads zip, extracts csv, loads into raw.trips_raw
```
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
dbt run
dbt test
dbt docs generate
dbt docs serve
```
- Profile name is `divvy_analytics`, target `local`, schema `analytics_dbt` (see profile config — locally this is typically `~/.dbt/profiles.yml`; Airflow uses `airflow/dbt_profiles/profiles.yml`).
- dbt connection vars come from the same Postgres env vars (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, plus `DBT_POSTGRES_HOST`/`DBT_POSTGRES_PORT`, defaulting to `localhost:5432`).
- Staging models materialize as views, marts as tables (`dbt/dbt_project.yml`).

### Airflow (from `airflow/`)
```bash
docker compose --env-file ../.env up --build -d   # requires Postgres already running (docker compose up -d from root)
docker compose --env-file ../.env exec airflow airflow dags trigger divvy_pipeline --conf '{"source_month":"202401"}'
docker compose --env-file ../.env down
```
- UI at `http://localhost:8080`, login `admin` / `airflow`.
- The Airflow container mounts the whole project at `/opt/divvy-analytics` and runs project commands (psql, dbt, the ingestion script) directly using that checkout — Airflow tasks are thin wrappers around the same commands listed above, not a separate implementation.
- Requires `data/csv/<source_month>-divvy-tripdata.csv` to already exist before triggering (the `check_raw_files` task fails fast otherwise).

## Notebooks

`notebooks/01_divvy_eda.ipynb` produces the charts in `images/` (rider volume/mix, trip duration trends, station concentration) — regenerate it if those visuals or the underlying marts change.
