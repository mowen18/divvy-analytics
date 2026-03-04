DROP TABLE IF EXISTS analytics.mart_trips_daily;

CREATE TABLE analytics.mart_trips_daily AS
SELECT
  started_date,
  started_month,
  rider_type,
  COUNT(*)::bigint AS trips,
  AVG(duration_seconds) / 60.0 AS avg_trip_minutes
FROM analytics.stg_trips
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_mart_trips_daily_started_date
  ON analytics.mart_trips_daily (started_date);
