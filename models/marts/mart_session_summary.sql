-- models/marts/mart_session_summary.sql
-- Purpose: Session-level aggregations to support session progression and degradation analytics

with session_data as (
    select
        race_id,
        driver_id,
        count(*) as total_laps,
        avg(lap_time_ms) as avg_session_pace_ms,
        min(lap_time_ms) as best_session_pace_ms,
        max(lap_time_ms) as worst_session_pace_ms,
        stddev(lap_time_ms) as session_pace_variation,
        -- Early vs late session splits
        avg(case when lap <= 3 then lap_time_ms end) as early_session_avg_ms,
        avg(case when lap > 6 then lap_time_ms end) as late_session_avg_ms,
        count(case when lap <= 3 then 1 end) as early_session_laps,
        count(case when lap > 6 then 1 end) as late_session_laps
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 0
    group by race_id, driver_id
    having count(*) >= 3
),
session_metrics as (
    select *,
        (session_pace_variation / avg_session_pace_ms) as session_consistency_coeff,
        (worst_session_pace_ms - best_session_pace_ms) as session_pace_range_ms,
        (late_session_avg_ms - early_session_avg_ms) as session_degradation_ms,
        case
            when (late_session_avg_ms - early_session_avg_ms) < 500 then 'Consistent'
            when (late_session_avg_ms - early_session_avg_ms) < 1500 then 'Moderate Decline'
            else 'Significant Decline'
        end as degradation_pattern
    from session_data
    where early_session_avg_ms is not null
        and late_session_avg_ms is not null
)

select
    race_id,
    driver_id,
    total_laps,
    round(avg_session_pace_ms, 0) as avg_session_pace_ms,
    round(best_session_pace_ms, 0) as best_session_pace_ms,
    round(session_pace_variation, 0) as session_pace_variation_ms,
    round(session_consistency_coeff, 4) as session_consistency_coeff,
    round(session_pace_range_ms, 0) as session_pace_range_ms,
    round(session_degradation_ms, 0) as session_degradation_ms,
    degradation_pattern,
    early_session_laps,
    late_session_laps
from session_metrics