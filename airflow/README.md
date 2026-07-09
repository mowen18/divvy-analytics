# Airflow Orchestration

This folder adds a small local Airflow layer around the existing Divvy workflow. It does not replace the current Python, Postgres, SQL, or dbt commands.

The DAG is designed to be rerunnable for a selected `source_month` during local development. Raw ingestion replaces rows only for that month, and the dbt incremental models delete+insert the same month's partitions, so re-triggering an already-loaded month produces identical results. This is intended for local development and portfolio demonstration, not as a production-grade incremental pipeline.

## DAG

`dags/divvy_pipeline.py` defines a manually triggered DAG named `divvy_pipeline`:

```text
check_raw_files
  >> load_raw_trips
  >> dbt_run
  >> dbt_test
  >> summarize_outputs
```

The DAG accepts two parameters:

- `source_month`: Divvy month in `YYYYMM` format. The default is `202401`.
- `full_refresh`: boolean, default `false`. When `true`, `dbt_run` rebuilds the incremental dbt models from scratch with `--full-refresh`.

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
username: admin
password: airflow
```

The login is created on container start and survives container recreation. Override it by setting `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD` in the project `.env`.

## Trigger The DAG

In the Airflow UI, enable and trigger `divvy_pipeline`. To run a different month, override the DAG parameter:

```json
{
  "source_month": "202402"
}
```

You can also trigger it from the `airflow/` folder:

```bash
docker compose --env-file ../.env exec airflow airflow dags trigger divvy_pipeline \
  --conf '{"source_month":"202401"}'
```

## What Airflow Runs

The tasks run the existing project commands from `/opt/divvy-analytics` inside the Airflow container:

```bash
python ingestion/load_divvy_month.py "$source_month"
cd dbt && dbt run --vars '{"source_month": "<source_month>"}'  # plus --full-refresh when the param is set
cd dbt && dbt test
```

The legacy SQL scripts (`sql/`) are no longer part of the DAG; they remain runnable manually as the frozen first version of the pipeline.

The final task prints row counts and recent metrics from:

- `analytics_dbt.mart_trips_daily`
- `analytics_dbt.mart_station_activity`
