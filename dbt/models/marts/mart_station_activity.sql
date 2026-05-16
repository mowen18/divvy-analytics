select
    start_station_name,

    count(*) as trip_starts,

    count(*) filter (
        where member_casual = 'member'
    ) as member_trip_starts,

    count(*) filter (
        where member_casual = 'casual'
    ) as casual_trip_starts,

    round(
        100.0 * count(*) / sum(count(*)) over (),
        4
    ) as pct_of_all_trip_starts,

    min(started_at) as first_trip_started_at,
    max(started_at) as last_trip_started_at

from {{ ref('stg_divvy_trips') }}

where start_station_name is not null

group by 1