-- Purpose:
-- Find lap deltas to detect under/overcuts and performance dips.
--
with data as (
    select * from {{ ref('int_stint_segmentation') }}
),
with_deltas as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        lag(lap_time_ms) over (
            partition by race_id, driver_id order by lap
        ) as prev_lap_time_ms
    from data
)

select
    *,
    lap_time_ms - prev_lap_time_ms as delta_ms
from with_deltas
where prev_lap_time_ms is not null
