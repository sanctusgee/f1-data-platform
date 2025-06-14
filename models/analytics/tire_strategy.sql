-- models/analytics/mart_tire_strategy_effectiveness.sql
-- Purpose: Tire Strategy Effectiveness - Analyzing tire strategies.
-- Which stint patterns actually work best

with stint_performance as (
    select
        race_id,
        driver_id,
        stint_number,
        total_laps,
        avg_lap_time_ms,
        case
            when total_laps <= 8 then 'Short Stint'
            when total_laps <= 15 then 'Medium Stint'
            else 'Long Stint'
        end as stint_length_category
    from {{ ref('mart_stint_summary') }}
    where total_laps >= 3
),
strategy_summary as (
    select
        race_id,
        driver_id,
        count(*) as total_stints,
        string_agg(stint_length_category, ' → ' order by stint_number) as strategy_pattern,
        avg(avg_lap_time_ms) as overall_avg_pace,
        min(avg_lap_time_ms) as best_stint_pace,
        max(avg_lap_time_ms) - min(avg_lap_time_ms) as pace_consistency_range
    from stint_performance
    group by race_id, driver_id
)

select
    strategy_pattern,
    count(*) as times_used,
    round(avg(overall_avg_pace), 0) as avg_overall_pace_ms,
    round(avg(pace_consistency_range), 0) as avg_pace_range_ms,
    round(avg(best_stint_pace), 0) as avg_best_stint_pace_ms,
    case
        when avg(pace_consistency_range) < 1000 then 'Consistent Strategy'
        when avg(pace_consistency_range) < 2000 then 'Moderate Variation'
        else 'High Variation'
    end as strategy_consistency
from strategy_summary
group by strategy_pattern
having count(*) >= 2  -- Only strategies used multiple times
order by avg_overall_pace_ms