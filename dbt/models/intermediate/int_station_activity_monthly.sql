-- Station activity at (source_month, start_station_name) grain. The all-time
-- station mart needs global aggregates that can't be updated one month at a
-- time, so the incremental grain lives here and mart_station_activity rolls
-- this table up.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_month',
    on_schema_change='fail'
) }}

select
    source_month,
    start_station_name,

    count(*) as trip_starts,

    count(*) filter (
        where member_casual = 'member'
    ) as member_trip_starts,

    count(*) filter (
        where member_casual = 'casual'
    ) as casual_trip_starts,

    min(started_at) as first_trip_started_at,
    max(started_at) as last_trip_started_at

from {{ ref('int_trips') }}

where start_station_name is not null
{% if is_incremental() %}
  and source_month = '{{ var("source_month") }}'
{% endif %}

group by 1, 2
