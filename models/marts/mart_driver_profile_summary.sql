-- models/marts/mart_driver_profile_summary.sql
-- Purpose: Driver baseline metrics to support multiple profiling analytics

with driver_baselines as (
    select
        driver_id,
        count(distinct race_id) as total_races,
        count(*) as total_laps,
        avg(lap_time_ms) as overall_avg_pace_ms,
        min(lap_time_ms) as overall_best_pace_ms,
        max(lap_time_ms) as overall_worst_pace_ms,
        stddev(lap_time_ms) as overall_pace_variation,
        -- Session type analysis
        avg(case when lap <= 3 then lap_time_ms end) as avg_early_session_pace_ms,
        avg(case when lap > 3 then lap_time_ms end) as avg_late_session_pace_ms,
        count(case when lap <= 3 then 1 end) as early_session_laps,
        count(case when lap > 3 then 1 end) as late_session_laps
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 50000 and 300000
        and lap > 0
    group by driver_id
    having count(distinct race_id) >= 2
),
driver_consistency as (
    select *,
        (overall_pace_variation / overall_avg_pace_ms) as overall_consistency_coeff,
        (overall_worst_pace_ms - overall_best_pace_ms) as overall_pace_range_ms,
        (avg_late_session_pace_ms - avg_early_session_pace_ms) as avg_session_degradation_ms,
        case
            when (overall_pace_variation / overall_avg_pace_ms) < 0.02 then 'Elite Consistency'
            when (overall_pace_variation / overall_avg_pace_ms) < 0.035 then 'High Consistency'
            when (overall_pace_variation / overall_avg_pace_ms) < 0.05 then 'Moderate Consistency'
            else 'Variable Performance'
        end as consistency_profile,
        case
            when (avg_late_session_pace_ms - avg_early_session_pace_ms) < 500 then 'Strong Endurance'
            when (avg_late_session_pace_ms - avg_early_session_pace_ms) < 1000 then 'Good Endurance'
            when (avg_late_session_pace_ms - avg_early_session_pace_ms) < 2000 then 'Moderate Endurance'
            else 'Poor Endurance'
        end as endurance_profile
    from driver_baselines
    where avg_early_session_pace_ms is not null
        and avg_late_session_pace_ms is not null
)

select
    driver_id,
    total_races,
    total_laps,
    round(overall_avg_pace_ms, 0) as overall_avg_pace_ms,
    round(overall_best_pace_ms, 0) as overall_best_pace_ms,
    round(overall_pace_variation, 0) as overall_pace_variation_ms,
    round(overall_consistency_coeff, 4) as overall_consistency_coeff,
    round(overall_pace_range_ms, 0) as overall_pace_range_ms,
    round(avg_session_degradation_ms, 0) as avg_session_degradation_ms,
    consistency_profile,
    endurance_profile,
    early_session_laps,
    late_session_laps
from driver_consistency
