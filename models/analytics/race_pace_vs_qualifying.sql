-- models/analytics/mart_race_vs_qualifying_pace.sql
-- Purpose: Race vs Qualifying Pace - Understanding different driver strengths
with session_classification as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        case
            when lap <= 3 then 'Qualifying Simulation'
            when lap > 3 then 'Race Simulation'
        end as session_type
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 0
),
pace_by_session as (
    select
        race_id,
        driver_id,
        session_type,
        count(*) as total_laps,
        avg(lap_time_ms) as avg_pace_ms,
        min(lap_time_ms) as best_pace_ms,
        stddev(lap_time_ms) as pace_variation
    from session_classification
    group by race_id, driver_id, session_type
    having count(*) >= 2
),
pace_comparison as (
    select
        q.race_id,
        q.driver_id,
        q.avg_pace_ms as qualifying_pace_ms,
        q.best_pace_ms as best_qualifying_ms,
        r.avg_pace_ms as race_pace_ms,
        r.best_pace_ms as best_race_ms,
        (r.avg_pace_ms - q.avg_pace_ms) as pace_delta_ms,
        (r.pace_variation / q.pace_variation) as consistency_ratio
    from pace_by_session q
    join pace_by_session r
        on q.race_id = r.race_id
        and q.driver_id = r.driver_id
        and q.session_type = 'Qualifying Simulation'
        and r.session_type = 'Race Simulation'
)

select
    driver_id,
    count(*) as sessions_analyzed,
    round(avg(qualifying_pace_ms), 0) as avg_qualifying_pace_ms,
    round(avg(race_pace_ms), 0) as avg_race_pace_ms,
    round(avg(pace_delta_ms), 0) as avg_pace_delta_ms,
    round(avg(consistency_ratio), 3) as race_vs_qual_consistency,
    case
        when avg(pace_delta_ms) < 1000 then 'Strong Race Pace'
        when avg(pace_delta_ms) < 2000 then 'Moderate Race Pace'
        else 'Qualifying Specialist'
    end as pace_profile
from pace_comparison
group by driver_id
having count(*) >= 3
order by avg_pace_delta_ms