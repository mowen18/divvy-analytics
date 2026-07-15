from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


COMMON_BASH = r"""
set -euo pipefail

PROJECT_DIR="${DIVVY_PROJECT_DIR:-/opt/divvy-analytics}"
PYTHON_BIN="${DIVVY_PYTHON_BIN:-$PROJECT_DIR/.venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python"
fi

cd "$PROJECT_DIR"

if [ -z "${DATABASE_URL:-}" ] && [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

: "${DATABASE_URL:?DATABASE_URL must be set in the Airflow environment or project .env}"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$PROJECT_DIR/airflow/dbt_profiles}"
SOURCE_MONTH="{{ params.source_month }}"
"""


with DAG(
    dag_id="divvy_pipeline",
    description="Orchestrates the local Divvy Postgres and dbt analytics workflow.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["divvy", "postgres", "dbt"],
    params={
        "source_month": Param(
            # Latest validated month of the 202407–202506 backfill. Updated by
            # hand when a new month is validated — not auto-detected, by design
            # (schedule=None, manually triggered pipeline).
            "202506",
            type="string",
            pattern=r"^\d{4}(0[1-9]|1[0-2])$",
            description="Divvy source-file month to ingest and process, formatted as YYYYMM.",
        ),
        "full_refresh": Param(
            False,
            type="boolean",
            description="Rebuild incremental dbt models from scratch (--full-refresh).",
        ),
    },
) as dag:
    load_raw_trips = BashOperator(
        task_id="load_raw_trips",
        bash_command=COMMON_BASH
        + r"""
"$PYTHON_BIN" ingestion/load_divvy_month.py "$SOURCE_MONTH"
""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=COMMON_BASH
        + r"""
cd "$PROJECT_DIR/dbt"
FULL_REFRESH_FLAG="{{ '--full-refresh' if params.full_refresh else '' }}"
dbt run --vars "{\"source_month\": \"$SOURCE_MONTH\"}" $FULL_REFRESH_FLAG
""",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=COMMON_BASH
        + r"""
cd "$PROJECT_DIR/dbt"
dbt test
""",
    )

    summarize_outputs = BashOperator(
        task_id="summarize_outputs",
        bash_command=COMMON_BASH
        + r"""
psql "$DATABASE_URL" <<'SQL'
\echo ''
\echo 'dbt mart row counts'
SELECT
  COUNT(*) AS mart_trips_daily_rows,
  COALESCE(SUM(total_trips), 0) AS total_trips,
  MIN(trip_date) AS first_trip_date,
  MAX(trip_date) AS last_trip_date
FROM analytics_dbt.mart_trips_daily;

SELECT
  COUNT(*) AS mart_station_activity_rows,
  COALESCE(SUM(trip_starts), 0) AS station_trip_starts
FROM analytics_dbt.mart_station_activity;

\echo ''
\echo 'latest daily trip metrics'
SELECT
  trip_date,
  total_trips,
  member_trips,
  casual_trips,
  avg_trip_duration_minutes
FROM analytics_dbt.mart_trips_daily
ORDER BY trip_date DESC
LIMIT 5;
SQL
""",
    )

    (
        load_raw_trips
        >> dbt_run
        >> dbt_test
        >> summarize_outputs
    )
