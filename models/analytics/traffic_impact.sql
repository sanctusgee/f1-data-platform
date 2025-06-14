-- models/analytics/mart_traffic_impact.sql
-- purpose: Quantifies how much traffic affects each driver's performance
with position_context as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        position,
        lag(position) over (
            partition by race_id, driver_id
            order by lap
        ) as prev_position,
        case
            when position <= 3 then 'Clear Air (P1-3)'
            when position <= 8 then 'Light Traffic (P4-8)'
            when position <= 15 then 'Heavy Traffic (P9-15)'
            else 'Backmarker Traffic (P16+)'
        end as traffic_category,
        case
            when position < lag(position) over (
                partition by race_id, driver_id order by lap
            ) then 'Overtaking'
            when position > lag(position) over (
                partition by race_id, driver_id order by lap
            ) then 'Being Overtaken'
            else 'Stable Position'
        end as position_change
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 1  -- Skip first lap
),
traffic_performance as (
    select
        driver_id,
        traffic_category,
        position_change,
        count(*) as total_laps,
        avg(lap_time_ms) as avg_pace_ms,
        min(lap_time_ms) as best_pace_ms,
        stddev(lap_time_ms) as pace_variation
    from position_context
    group by driver_id, traffic_category, position_change
    having count(*) >= 5
),
clean_air_baseline as (
    select
        driver_id,
        avg(avg_pace_ms) as clean_air_pace_ms
    from traffic_performance
    where traffic_category = 'Clear Air (P1-3)'
    group by driver_id
)

select
    tp.driver_id,
    tp.traffic_category,
    tp.position_change,
    tp.total_laps,
    round(tp.avg_pace_ms, 0) as avg_pace_ms,
    round(tp.pace_variation, 0) as pace_variation_ms,
    round(tp.avg_pace_ms - cab.clean_air_pace_ms, 0) as traffic_penalty_ms,
    case
        when tp.avg_pace_ms - cab.clean_air_pace_ms < 500 then 'Minimal Impact'
        when tp.avg_pace_ms - cab.clean_air_pace_ms < 1500 then 'Moderate Impact'
        else 'Significant Impact'
    end as traffic_impact_rating
from traffic_performance tp
left join clean_air_baseline cab on tp.driver_id = cab.driver_id
where cab.clean_air_pace_ms is not null
order by tp.driver_id, tp.traffic_category, tp.position_change