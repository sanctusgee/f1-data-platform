-- file: stg_pit_stops.sql (simplified test version)
-- Purpose:
-- Standardize pit stop data, normalize types, derive race_id, and convert durations to milliseconds.

{{ config(materialized='ephemeral') }}

with raw as (
    select * from {{ source('raw', 'raw_pit_stops') }}
)

select
    cast(race_id as varchar) as race_id,
    cast(driver_id as varchar) as driver_id,
    cast(stop as integer) as stop_number,
    cast(lap as integer) as lap,
    cast(time as varchar) as pit_time_str,
    {{ convert_time_string_to_ms('duration') }} as pit_duration_ms,
    current_timestamp as processed_at
from raw
where duration is not null and trim(duration) != ''