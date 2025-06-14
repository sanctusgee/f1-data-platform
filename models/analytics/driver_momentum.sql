-- models/analytics/mart_driver_momentum.sql
-- Purpose: idwentifies drivers who build momentum vs those who lose steam

with lap_trends as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        lag(lap_time_ms, 1) over (
            partition by race_id, driver_id order by lap
        ) as prev_lap_ms,
        lag(lap_time_ms, 2) over (
            partition by race_id, driver_id order by lap
        ) as prev_lap_2_ms,
        case
            when lap_time_ms < lag(lap_time_ms, 1) over (
                partition by race_id, driver_id order by lap
            ) then 'Improving'
            when lap_time_ms > lag(lap_time_ms, 1) over (
                partition by race_id, driver_id order by lap
            ) then 'Declining'
            else 'Stable'
        end as pace_trend
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 0
),
momentum_sequences as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        pace_trend,
        -- Identify consecutive improving/declining laps
        case
            when pace_trend = lag(pace_trend, 1) over (
                partition by race_id, driver_id order by lap
            )
            and pace_trend = lag(pace_trend, 2) over (
                partition by race_id, driver_id order by lap
            ) then 'Strong Momentum'
            when pace_trend = lag(pace_trend, 1) over (
                partition by race_id, driver_id order by lap
            ) then 'Building Momentum'
            else 'Inconsistent'
        end as momentum_pattern,
        -- Calculate improvement/decline magnitude
        case
            when prev_lap_ms is not null then
                abs(lap_time_ms - prev_lap_ms)
            else 0
        end as pace_change_magnitude_ms
    from lap_trends
    where prev_lap_ms is not null
),
momentum_analysis as (
    select
        driver_id,
        momentum_pattern,
        pace_trend,
        count(*) as total_occurrences,
        avg(pace_change_magnitude_ms) as avg_change_magnitude_ms,
        max(pace_change_magnitude_ms) as max_change_magnitude_ms
    from momentum_sequences
    where momentum_pattern != 'Inconsistent'
    group by driver_id, momentum_pattern, pace_trend
    having count(*) >= 3
)

select
    driver_id,
    momentum_pattern,
    pace_trend,
    total_occurrences,
    round(avg_change_magnitude_ms, 0) as avg_pace_change_ms,
    round(max_change_magnitude_ms, 0) as max_pace_change_ms,
    case
        when momentum_pattern = 'Strong Momentum' and pace_trend = 'Improving' then 'Hot Streak Specialist'
        when momentum_pattern = 'Strong Momentum' and pace_trend = 'Declining' then 'Prone to Fade'
        when momentum_pattern = 'Building Momentum' and pace_trend = 'Improving' then 'Progressive Builder'
        when momentum_pattern = 'Building Momentum' and pace_trend = 'Declining' then 'Gradual Decline'
        else 'Mixed Pattern'
    end as momentum_profile
from momentum_analysis
order by driver_id, momentum_pattern, pace_trend