-- file: int_stint_degradation.sql
-- Purpose:
-- Measure average pace trend per stint—basic degradation signal.
--
with data as (
    select * from {{ ref('int_stint_segmentation') }}
    where lap_time_ms is not null and lap is not null
),
with_stats as (
    select
        race_id,
        driver_id,
        stint_number,
        min(lap) as start_lap,
        max(lap) as end_lap,
        count(*) as total_laps,
        avg(lap_time_ms) as avg_pace,
        null::numeric as avg_pace_trend_ms  -- Temporarily remove corr()
    from data
    group by race_id, driver_id, stint_number
    having count(*) >= 3
)

select * from with_stats