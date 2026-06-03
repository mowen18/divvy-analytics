# Airflow Orchestration

This folder adds a small local Airflow layer around the existing Divvy workflow. It does not replace the current Python, Postgres, SQL, or dbt commands.

The DAG is designed to be rerunnable for a selected `source_month` during local development. Raw ingestion replaces rows only for that month, and the DAG resets downstream dbt objects before rebuilding them. This is intended for local development and portfolio demonstration, not as a production-grade incremental pipeline.

## DAG

`dags/divvy_pipeline.py` defines a manually triggered DAG named `divvy_pipeline`:

```text
check_raw_files
  >> load_raw_trips
  >> reset_dbt_schema
  >> run_sql_staging
  >> dbt_run
  >> dbt_test
  >> summarize_outputs
```

The DAG accepts one parameter:

- `source_month`: Divvy month in `YYYYMM` format. The default is `202401`.

## Prerequisites

From the project root, start the existing Postgres service:

```bash
docker compose up -d
```

Confirm `.env` exists in the project root and includes:

```bash
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DB=...
DATABASE_URL=postgresql://...@localhost:5432/...
```

Confirm the raw Divvy CSV exists for the month you want to run:

```bash
ls -lh data/csv/202401-divvy-tripdata.csv
```

## Start Airflow

From this `airflow/` folder:

```bash
docker compose --env-file ../.env up --build
```

Open Airflow at [http://localhost:8080](http://localhost:8080).

Default local login:

```text
username: airflow
password: airflow
```

## Trigger The DAG

In the Airflow UI, enable and trigger `divvy_pipeline`. To run a different month, override the DAG parameter:

```json
{
  "source_month": "202402"
}
```

You can also trigger it from inside the Airflow container:

```bash
docker compose --env-file ../.env exec airflow airflow dags trigger divvy_pipeline \
  --conf '{"source_month":"202401"}'
```

## What Airflow Runs

The tasks run the existing project commands from `/opt/divvy-analytics` inside the Airflow container:

```bash
python ingestion/load_divvy_month.py "$source_month"
psql "$DATABASE_URL" -c "DROP SCHEMA IF EXISTS analytics_dbt CASCADE;"
psql "$DATABASE_URL" -f sql/00_init.sql
psql "$DATABASE_URL" -f sql/10_stg_trips.sql
cd dbt && dbt run
cd dbt && dbt test
```

The final task prints row counts and recent metrics from:

- `analytics_dbt.mart_trips_daily`
- `analytics_dbt.mart_station_activity`
