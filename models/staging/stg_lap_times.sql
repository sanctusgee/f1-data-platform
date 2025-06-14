-- file: stg_lap_times.sql
-- Purpose:
-- Standardize column names and types, handle weird API formats, clean time strings, and prep for modeling.

{{ config(
    materialized='ephemeral'
) }}

with raw as (
    select * from {{ source('raw', 'raw_lap_times') }}
)

select
    -- Create a unique race_id using season and round
--     concat(cast(season as varchar), '_', cast(round as varchar)) as race_id,
--     cast(race_id as integer) as race_id,
    cast(race_id as varchar) as race_id,
    cast(driver_id as varchar) as driver_id,
    cast(lap_number as integer) as lap,
    cast(position as integer) as position,
    cast(circuit as varchar) as circuit,
    -- Convert "1:35.123" to milliseconds
    {{ convert_time_string_to_ms('time') }} as lap_time_ms,

    current_timestamp as processed_at
from raw
where time is not null
