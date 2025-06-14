-- file: models/analytics/mart_qualifying_consistency.sql
with lap_stats as (
    select
        race_id,
        driver_id,
        count(*) as total_laps,
        avg(lap_time_ms::numeric) as avg_pace_ms,
        stddev(lap_time_ms::numeric) as pace_variation,
        min(lap_time_ms) as fastest_lap_ms,
        max(lap_time_ms) as slowest_lap_ms
    from {{ ref('stg_lap_times') }}
    where lap_time_ms > 0
        and lap > 0  -- Exclude outlap
        and lap_time_ms between 60000 and 180000  -- Realistic F1 times
    group by race_id, driver_id
    having count(*) >= 1  -- Minimum laps for meaningful analysis
),
consistency_metrics as (
    select *,
        (pace_variation / avg_pace_ms) as consistency_coefficient,
        (slowest_lap_ms - fastest_lap_ms) as pace_range_ms,
        case
            when (pace_variation / avg_pace_ms) < 0.015 then 'Elite Consistency'
            when (pace_variation / avg_pace_ms) < 0.025 then 'High Consistency'
            when (pace_variation / avg_pace_ms) < 0.035 then 'Moderate Consistency'
            else 'Variable Performance'
        end as consistency_rating
    from lap_stats
)

select
    driver_id,
    round(avg(avg_pace_ms), 0) as season_avg_pace_ms,
    round(avg(consistency_coefficient), 4) as avg_consistency_coeff,
    consistency_rating,
    count(*) as races_analyzed,
    round(avg(pace_range_ms), 0) as avg_pace_range_ms
from consistency_metrics
group by driver_id, consistency_rating
-- Fixed: Use HAVING instead of WHERE for aggregated columns
having avg(avg_pace_ms) is not null
    and avg(pace_variation) is not null
    and avg(pace_variation) > 0  -- Avoid division by zero
    and avg(consistency_coefficient) is not null
order by avg(consistency_coefficient)