-- Trips by month
SELECT
  started_month,
  COUNT(*) AS trips
FROM analytics.stg_trips
GROUP BY 1
ORDER BY 1;

-- Member vs casual by month
SELECT
  started_month,
  rider_type,
  COUNT(*) AS trips
FROM analytics.stg_trips
GROUP BY 1, 2
ORDER BY 1, 2;

-- Trips by day of week
SELECT
  EXTRACT(ISODOW FROM started_at)::int AS day_of_week_num,
  TO_CHAR(started_at, 'Dy') AS day_of_week,
  COUNT(*) AS trips
FROM analytics.stg_trips
GROUP BY 1, 2
ORDER BY 1;

-- Trips by hour
SELECT
  EXTRACT(HOUR FROM started_at)::int AS hour_of_day,
  COUNT(*) AS trips
FROM analytics.stg_trips
GROUP BY 1
ORDER BY 1;

-- Average + median duration (minutes) by rider_type
SELECT
  rider_type,
  AVG(duration_seconds) / 60.0 AS avg_duration_minutes,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds / 60.0) AS median_duration_minutes
FROM analytics.stg_trips
GROUP BY 1
ORDER BY 1;

-- Top 20 start stations
SELECT
  start_station_name,
  COUNT(*) AS trips
FROM analytics.stg_trips
WHERE start_station_name IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC, 1
LIMIT 20;
