-- Source of truth for what counts as a valid Divvy trip.
-- Ports sql/10_stg_trips.sql exactly: NULLIF('') is applied before every cast
-- because casting '' to timestamp errors in Postgres.
-- Must stay projections + row filters only (no aggregation, window functions,
-- or DISTINCT) so Postgres inlines the view and pushes the source_month
-- filter from int_trips down to the raw.trips_raw scan.

select
    source_month,

    nullif(ride_id, '')                         as ride_id,
    nullif(rideable_type, '')                   as rideable_type,

    nullif(started_at, '')::timestamp           as started_at,
    nullif(ended_at, '')::timestamp             as ended_at,

    nullif(start_station_name, '')              as start_station_name,
    nullif(start_station_id, '')                as start_station_id,
    nullif(end_station_name, '')                as end_station_name,
    nullif(end_station_id, '')                  as end_station_id,

    nullif(start_lat, '')::double precision     as start_lat,
    nullif(start_lng, '')::double precision     as start_lng,
    nullif(end_lat, '')::double precision       as end_lat,
    nullif(end_lng, '')::double precision       as end_lng,

    case
        when lower(nullif(member_casual, '')) in ('member', 'casual')
            then lower(member_casual)
        else null
    end                                         as member_casual,

    extract(epoch from (nullif(ended_at, '')::timestamp - nullif(started_at, '')::timestamp))::bigint
                                                as duration_seconds,

    date(nullif(started_at, '')::timestamp)     as started_date,
    date_trunc('month', nullif(started_at, '')::timestamp)::date
                                                as started_month

from {{ source('divvy', 'trips_raw') }}
where
    nullif(started_at, '') is not null
    and nullif(ended_at, '') is not null
    and nullif(ended_at, '')::timestamp > nullif(started_at, '')::timestamp
    -- keep plausible rides only
    and (nullif(ended_at, '')::timestamp - nullif(started_at, '')::timestamp)
        between interval '1 minute' and interval '24 hours'
