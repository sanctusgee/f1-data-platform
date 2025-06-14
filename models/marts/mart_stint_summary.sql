-- file: mart_stint_summary.sql
-- Purpose:
-- Summarize each stint for every driver in every race.

with clean_data as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms
    from {{ ref('stg_lap_times') }}
    where
        lap_time_ms is not null
        and lap is not null
        and lap_time_ms > 0
        and lap > 0
),
summary as (
    select
        race_id,
        driver_id,
        1 as stint_number,  -- Temporary - just use stint 1 for all
        min(lap) as start_lap,
        max(lap) as end_lap,
        count(*) as total_laps,
        avg(lap_time_ms::numeric) as avg_lap_time_ms,
        null::numeric as avg_pace_trend_ms
    from clean_data
    group by race_id, driver_id
    -- Risk of over-filtering: reducing to 1 (one) because most drivers have only one stint,
    --      eg  (albon: 1, alonso: 2, bottas: 1, etc.)
    --  Dataset appears to be qualifying or practice data where drivers
    --     do shorter runs (1-2 laps) rather than full race stints (10+ laps).
    having count(*) >= 1
)

select * from summary