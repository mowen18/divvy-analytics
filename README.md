# Divvy Analytics Portfolio Project

This project ingests Chicago Divvy trip data into Postgres, cleans it into a staging model, and builds analytical outputs for KPI exploration.

## How to run

1. Start Postgres:
   ```bash
   docker compose up -d
   ```
2. Activate the virtual environment:
   ```bash
   source .venv/bin/activate
   ```
3. Load data for a month (example: January 2024):
   ```bash
   python ingestion/load_divvy_month.py 202401
   ```
4. Run SQL models:
   ```bash
   psql "$DATABASE_URL" -f sql/00_init.sql
   psql "$DATABASE_URL" -f sql/10_stg_trips.sql
   psql "$DATABASE_URL" -f sql/30_mart_trips_daily.sql
   ```
5. Run KPI queries:
   ```bash
   psql "$DATABASE_URL" -f sql/20_kpis.sql
   ```

## Key outputs

- `analytics.stg_trips`:
  - Cleaned trip-level model from `raw.trips_raw`.
  - Standardized rider type (`member` / `casual`).
  - Derived fields like `duration_seconds`, `started_date`, and `started_month`.
  - Filters to plausible rides (between 1 minute and 24 hours, and `ended_at > started_at`).

- `analytics.mart_trips_daily`:
  - Daily aggregated mart at grain: `(started_date, started_month, rider_type)`.
  - Includes `trips` and `avg_trip_minutes` for trend reporting.

- KPI query set (`sql/20_kpis.sql`):
  - Trips by month
  - Member vs casual by month
  - Trips by day of week
  - Trips by hour
  - Average and median trip duration (minutes) by rider type
  - Top 20 start stations

## Visuals

These charts come from `notebooks/01_divvy_eda.ipynb` and summarize rider volume, mix, and trip duration trends from `analytics.mart_trips_daily`.

### Trips Over Time by Rider Type

![Trips Over Time by Rider Type](images/trips_over_time_by_rider_type.png)

### Rider Type Share Over Time

![Rider Type Share Over Time](images/rider_type_share_over_time.png)

### Average Trip Minutes Over Time by Rider Type

![Average Trip Minutes Over Time by Rider Type](images/avg_trip_minutes_over_time.png)
