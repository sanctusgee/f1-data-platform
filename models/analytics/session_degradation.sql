-- models/analytics/mart_session_degradation.sql
-- Purpose:: Session Degradation - How drivers maintain performance over longer runs
with lap_sequence as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        row_number() over (
            partition by race_id, driver_id
            order by lap
        ) as lap_sequence,
        case
            when row_number() over (partition by race_id, driver_id order by lap) <= 3 then 'Early'
            when row_number() over (partition by race_id, driver_id order by lap) <= 6 then 'Middle'
            else 'Late'
        end as session_phase
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 0
),
phase_performance as (
    select
        race_id,
        driver_id,
        session_phase,
        count(*) as laps_in_phase,
        avg(lap_time_ms) as avg_pace_ms,
        min(lap_time_ms) as best_pace_ms,
        stddev(lap_time_ms) as pace_variation
    from lap_sequence
    group by race_id, driver_id, session_phase
    having count(*) >= 2
),
degradation_analysis as (
    select
        early.race_id,
        early.driver_id,
        early.avg_pace_ms as early_pace_ms,
        middle.avg_pace_ms as middle_pace_ms,
        late.avg_pace_ms as late_pace_ms,
        (middle.avg_pace_ms - early.avg_pace_ms) as early_to_middle_delta,
        (late.avg_pace_ms - middle.avg_pace_ms) as middle_to_late_delta,
        (late.avg_pace_ms - early.avg_pace_ms) as total_degradation_ms
    from phase_performance early
    join phase_performance middle
        on early.race_id = middle.race_id
        and early.driver_id = middle.driver_id
        and early.session_phase = 'Early'
        and middle.session_phase = 'Middle'
    join phase_performance late
        on middle.race_id = late.race_id
        and middle.driver_id = late.driver_id
        and late.session_phase = 'Late'
)

select
    driver_id,
    count(*) as sessions_analyzed,
    round(avg(early_pace_ms), 0) as avg_early_pace_ms,
    round(avg(late_pace_ms), 0) as avg_late_pace_ms,
    round(avg(total_degradation_ms), 0) as avg_total_degradation_ms,
    round(avg(early_to_middle_delta), 0) as early_middle_degradation_ms,
    round(avg(middle_to_late_delta), 0) as middle_late_degradation_ms,
    case
        when avg(total_degradation_ms) < 500 then 'Excellent Endurance'
        when avg(total_degradation_ms) < 1000 then 'Good Endurance'
        when avg(total_degradation_ms) < 2000 then 'Moderate Endurance'
        else 'Poor Endurance'
    end as endurance_rating
from degradation_analysis
group by driver_id
having count(*) >= 3
order by avg_total_degradation_ms