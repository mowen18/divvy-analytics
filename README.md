# Divvy Analytics Portfolio Project

This project ingests Chicago Divvy trip data into Postgres, cleans it into a staging model, and builds analytical outputs for KPI exploration.

The project now includes the original SQL-based workflow, a newer dbt analytics layer for model dependencies, tests, and documentation, and a local Airflow orchestration extension.

## dbt Analytics Layer

dbt Core workflow for transforming staged Divvy trip data into analytics-ready models.

### dbt model flow

```text
analytics.stg_trips
        ↓ source()
analytics_dbt.stg_divvy_trips
        ↓ ref()
analytics_dbt.mart_trips_daily
analytics_dbt.mart_station_activity
```

### dbt lineage graph

The dbt docs lineage graph shows the dependency flow from the existing staged trip table into the dbt staging model and downstream marts.

![dbt lineage graph](images/dbt_lineage_graph.png)


### dbt models

- `stg_divvy_trips`: dbt staging view built from the existing `analytics.stg_trips` table.
- `mart_trips_daily`: daily trip metrics including total trips, member trips, casual trips, and average trip duration.
- `mart_station_activity`: station-level trip start metrics used for station concentration analysis.

The original SQL files are kept as the first version of the pipeline, while the `dbt/` folder shows the newer analytics engineering workflow with model dependencies, tests, and documentation.

### Common dbt commands

From the `dbt/` directory:

```bash
dbt debug
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Airflow Orchestration

Local Airflow extension for orchestrating the existing Divvy pipeline. The DAG lives at `airflow/dags/divvy_pipeline.py` and coordinates the current Python ingestion, Postgres SQL, and dbt steps without replacing that workflow.

```text
check_raw_files
  → load_raw_trips
  → reset_dbt_schema
  → run_sql_staging
  → dbt_run
  → dbt_test
  → summarize_outputs
```

The DAG is rerunnable for a selected `source_month`: raw ingestion replaces rows for that month, then Airflow resets and rebuilds downstream dbt objects before running dbt.

To run it locally, start Postgres from the project root:

```bash
docker compose up -d
```

Then start Airflow:

```bash
cd airflow
docker compose --env-file ../.env up --build -d
```

Open `http://localhost:8080`, log in with `admin / airflow`, and trigger the `divvy_pipeline` DAG through the UI. You can also trigger it from the Airflow folder with:

```bash
docker compose --env-file ../.env exec airflow airflow dags trigger divvy_pipeline --conf '{"source_month":"202401"}'
```

To stop Airflow:

```bash
docker compose --env-file ../.env down
```

Then stop Postgres from the project root:

```bash
docker compose down
```

See `airflow/README.md` for more detailed Airflow setup and usage notes.

## How to run

1. Start Postgres:

   ```bash
   docker compose up -d
   ```

2. Activate the virtual environment:

   ```bash
   source .venv/bin/activate
   ```

3. Load data for a month, for example January 2024:

   ```bash
   python ingestion/load_divvy_month.py 202401
   ```

4. Run the original SQL models:

   ```bash
   psql "$DATABASE_URL" -f sql/00_init.sql
   psql "$DATABASE_URL" -f sql/10_stg_trips.sql
   psql "$DATABASE_URL" -f sql/30_mart_trips_daily.sql
   ```

5. Run dbt models and tests:

   ```bash
   cd dbt
   dbt run
   dbt test
   ```

6. Optional: generate and view dbt docs:

   ```bash
   dbt docs generate
   dbt docs serve
   ```

7. Run KPI queries:

   ```bash
   cd ..
   psql "$DATABASE_URL" -f sql/20_kpis.sql
   ```

## Key outputs

### Original SQL models

- `analytics.stg_trips`:
  - Cleaned trip-level model from `raw.trips_raw`.
  - Standardized rider type (`member` / `casual`).
  - Derived fields like `duration_seconds`, `started_date`, and `started_month`.
  - Filters to plausible rides between 1 minute and 24 hours where `ended_at > started_at`.

- `analytics.mart_trips_daily`:
  - Daily aggregated mart at grain: `(started_date, started_month, rider_type)`.
  - Includes `trips` and `avg_trip_minutes` for trend reporting.

### dbt models

- `analytics_dbt.stg_divvy_trips`:
  - dbt-managed staging view built from `analytics.stg_trips`.
  - Standardizes the rider type column as `member_casual`.
  - Includes dbt tests for key fields like `ride_id`, `started_at`, and `member_casual`.

- `analytics_dbt.mart_trips_daily`:
  - Daily trip metrics aggregated from the dbt staging model.
  - Includes total trips, member trips, casual trips, and average trip duration.

- `analytics_dbt.mart_station_activity`:
  - Station-level trip start metrics.
  - Supports station concentration and top-start-station analysis.

### KPI query set

The KPI query file `sql/20_kpis.sql` includes:

- Trips by month
- Member vs casual trips by month
- Trips by day of week
- Trips by hour
- Average and median trip duration by rider type
- Top 20 start stations

## Visuals

These charts come from `notebooks/01_divvy_eda.ipynb` and summarize rider volume, rider mix, trip duration trends, and station activity.

### Trips Over Time by Rider Type

![Trips Over Time by Rider Type](images/trips_over_time_by_rider_type.png)

### Rider Type Share Over Time

![Rider Type Share Over Time](images/rider_type_share_over_time.png)

### Average Trip Minutes Over Time by Rider Type

![Average Trip Minutes Over Time by Rider Type](images/avg_trip_minutes_over_time.png)

### Station Concentration of Divvy Trip Starts

![Station Concentration of Divvy Trip Starts](images/station_concentration_of_divvy_trip_starts.png)
