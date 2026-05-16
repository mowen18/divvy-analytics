select
    date(started_at) as trip_date,
    count(*) as total_trips,

    count(*) filter (where member_casual = 'member') as member_trips,
    count(*) filter (where member_casual = 'casual') as casual_trips,

    round(
        avg(extract(epoch from (ended_at - started_at)) / 60.0),
        2
    ) as avg_trip_duration_minutes

from {{ ref('stg_divvy_trips') }}

where started_at is not null
  and ended_at is not null
  and ended_at > started_at

group by 1
order by 1