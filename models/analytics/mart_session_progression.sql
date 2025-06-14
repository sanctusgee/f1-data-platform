-- models/analytics/mart_session_progression.sql
with session_performance as (
    select
        race_id,
        driver_id,
        lap as attempt_number,
        lap_time_ms,
        row_number() over (
            partition by race_id, driver_id
            order by lap
        ) as attempt_sequence,
        min(lap_time_ms) over (
            partition by race_id, driver_id
        ) as personal_best_ms
    from {{ ref('stg_lap_times') }}
    where lap_time_ms > 0
        and lap_time_ms between 60000 and 180000
),
improvement_analysis as (
    select *,
        (lap_time_ms - personal_best_ms) as gap_to_personal_best_ms,
        case
            when lap_time_ms = personal_best_ms then 'Personal Best'
            when lap_time_ms - personal_best_ms < 500 then 'Within 0.5s'
            when lap_time_ms - personal_best_ms < 1000 then 'Within 1.0s'
            else 'Off Pace'
        end as pace_category,
        case
            when attempt_sequence = 1 then 'First Attempt'
            when attempt_sequence <= 2 then 'Early Attempts'
            else 'Later Attempts'
        end as pressure_phase
    from session_performance
),
pressure_response as (
    select
        driver_id,
        pressure_phase,
        count(*) as total_attempts,
        avg(gap_to_personal_best_ms) as avg_gap_to_best_ms,
        sum(case when pace_category = 'Personal Best' then 1 else 0 end) as best_laps,
        sum(case when pace_category in ('Personal Best', 'Within 0.5s') then 1 else 0 end) as strong_laps
    from improvement_analysis
    group by driver_id, pressure_phase
)

select
    driver_id,
    pressure_phase,
    total_attempts,
    round(avg_gap_to_best_ms, 0) as avg_gap_to_best_ms,
    best_laps,
    strong_laps,
    round(strong_laps::numeric / total_attempts::numeric * 100, 1) as strong_lap_percentage,
    case
        when strong_laps::numeric / total_attempts::numeric > 0.7 then 'Clutch Performer'
        when strong_laps::numeric / total_attempts::numeric > 0.5 then 'Reliable'
        else 'Inconsistent Under Pressure'
    end as pressure_rating
from pressure_response
order by driver_id, pressure_phase