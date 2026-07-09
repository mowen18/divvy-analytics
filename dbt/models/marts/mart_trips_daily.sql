{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='trip_date',
    on_schema_change='fail'
) }}

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
where source_month = '{{ var("source_month") }}'
{% endif %}

group by 1
