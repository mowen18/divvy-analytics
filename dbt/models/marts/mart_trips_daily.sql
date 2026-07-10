{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='trip_date',
    on_schema_change='fail'
) }}

-- A month's raw file can include trips that started on the last day of the
-- previous month, so a trip_date near a boundary draws from two source_months.
-- Incremental runs therefore recompute every trip_date touched by the current
-- month's batch from ALL source_months — filtering the aggregation itself to
-- one source_month would replace boundary dates with partial counts.

select
    date(started_at) as trip_date,
    count(*) as total_trips,

    count(*) filter (where member_casual = 'member') as member_trips,
    count(*) filter (where member_casual = 'casual') as casual_trips,

    round(
        avg(extract(epoch from (ended_at - started_at)) / 60.0),
        2
    ) as avg_trip_duration_minutes

from {{ ref('int_trips') }}

{% if is_incremental() %}
where date(started_at) in (
    select distinct date(started_at)
    from {{ ref('int_trips') }}
    where source_month = '{{ var("source_month") }}'
)
{% endif %}

group by 1
