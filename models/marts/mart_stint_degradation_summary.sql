-- file: mart_stint_degradation_summary.sql
-- Purpose:
-- Capture degradation signal per stint using correlation-based trend.

select
    race_id,
    driver_id,
    stint_number,
    total_laps,
    avg_pace,
    avg_pace_trend_ms
from {{ ref('int_stint_degradation') }}
where
    total_laps >= 3  -- Only include stints with enough data points
    and avg_pace is not null
    and avg_pace_trend_ms is not null