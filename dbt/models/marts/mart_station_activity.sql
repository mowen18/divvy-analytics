-- All-time station rollup over the monthly intermediate. Stays a plain table:
-- the window percentage and all-time min/max need every month, but the input
-- is already aggregated to (month, station), so the full rebuild is cheap.
-- ::bigint casts keep the count columns' types identical to the previous
-- count(*)-based version (sum(bigint) returns numeric in Postgres).

select
    start_station_name,

    sum(trip_starts)::bigint as trip_starts,

    sum(member_trip_starts)::bigint as member_trip_starts,

    sum(casual_trip_starts)::bigint as casual_trip_starts,

    round(
        100.0 * sum(trip_starts) / sum(sum(trip_starts)) over (),
        4
    ) as pct_of_all_trip_starts,

    min(first_trip_started_at) as first_trip_started_at,
    max(last_trip_started_at) as last_trip_started_at

from {{ ref('int_station_activity_monthly') }}

group by 1
