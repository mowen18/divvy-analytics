-- Materializes the staging view at trip grain, one source_month at a time.
-- delete+insert with unique_key=source_month gives partition-replace
-- semantics: the whole month is deleted and reinserted as a unit, mirroring
-- how ingestion loads raw.trips_raw. This is the only model that scans raw;
-- everything downstream refs int_trips, never the staging view.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_month',
    on_schema_change='fail'
) }}

select *
from {{ ref('stg_divvy_trips') }}
{% if is_incremental() %}
where source_month = '{{ var("source_month") }}'
{% endif %}
