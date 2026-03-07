DROP TABLE IF EXISTS analytics.stg_trips;

CREATE TABLE analytics.stg_trips AS
SELECT
  source_month,

  NULLIF(ride_id, '')                         AS ride_id,
  NULLIF(rideable_type, '')                   AS rideable_type,

  NULLIF(started_at, '')::timestamp           AS started_at,
  NULLIF(ended_at, '')::timestamp             AS ended_at,

  NULLIF(start_station_name, '')              AS start_station_name,
  NULLIF(start_station_id, '')                AS start_station_id,
  NULLIF(end_station_name, '')                AS end_station_name,
  NULLIF(end_station_id, '')                  AS end_station_id,

  NULLIF(start_lat, '')::double precision     AS start_lat,
  NULLIF(start_lng, '')::double precision     AS start_lng,
  NULLIF(end_lat, '')::double precision       AS end_lat,
  NULLIF(end_lng, '')::double precision       AS end_lng,

  CASE
    WHEN lower(NULLIF(member_casual,'')) IN ('member', 'casual')
      THEN lower(member_casual)
    ELSE NULL
  END                                         AS rider_type,

  EXTRACT(EPOCH FROM (NULLIF(ended_at,'')::timestamp - NULLIF(started_at,'')::timestamp))::bigint
                                              AS duration_seconds,

  DATE(NULLIF(started_at,'')::timestamp)      AS started_date,
  DATE_TRUNC('month', NULLIF(started_at,'')::timestamp)::date
                                              AS started_month
FROM raw.trips_raw
WHERE
  NULLIF(started_at,'') IS NOT NULL
  AND NULLIF(ended_at,'') IS NOT NULL
  AND NULLIF(ended_at,'')::timestamp > NULLIF(started_at,'')::timestamp
  -- keep plausible rides (adjust later if you want)
  AND (NULLIF(ended_at,'')::timestamp - NULLIF(started_at,'')::timestamp) BETWEEN interval '1 minute' AND interval '24 hours';
