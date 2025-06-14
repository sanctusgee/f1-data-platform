-- models/marts/mart_position_summary.sql
-- Purpose: Position-based aggregations to support traffic impact analytics

with position_data as (
    select
        race_id,
        driver_id,
        position,
        lap,
        lap_time_ms,
        lag(position) over (
            partition by race_id, driver_id
            order by lap
        ) as prev_position,
        case
            when position <= 3 then 'Clear Air (P1-3)'
            when position <= 8 then 'Light Traffic (P4-8)'
            when position <= 15 then 'Heavy Traffic (P9-15)'
            else 'Backmarker Traffic (P16+)'
        end as traffic_category
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 50000 and 300000
        and lap > 1  -- Skip formation lap
        and position is not null
),
position_changes as (
    select *,
        case
            when position < prev_position then 'Overtaking'
            when position > prev_position then 'Being Overtaken'
            else 'Stable Position'
        end as position_change_type
    from position_data
    where prev_position is not null
),
traffic_performance as (
    select
        driver_id,
        traffic_category,
        position_change_type,
        count(*) as total_laps,
        avg(lap_time_ms) as avg_pace_ms,
        min(lap_time_ms) as best_pace_ms,
        stddev(lap_time_ms) as pace_variation_ms,
        avg(position) as avg_position
    from position_changes
    group by driver_id, traffic_category, position_change_type
    having count(*) >= 3
),
clean_air_baseline as (
    select
        driver_id,
        avg(avg_pace_ms) as clean_air_pace_ms,
        avg(pace_variation_ms) as clean_air_variation_ms
    from traffic_performance
    where traffic_category = 'Clear Air (P1-3)'
    group by driver_id
)

select
    tp.driver_id,
    tp.traffic_category,
    tp.position_change_type,
    tp.total_laps,
    round(tp.avg_pace_ms, 0) as avg_pace_ms,
    round(tp.best_pace_ms, 0) as best_pace_ms,
    round(tp.pace_variation_ms, 0) as pace_variation_ms,
    round(tp.avg_position, 1) as avg_position,
    round(cab.clean_air_pace_ms, 0) as driver_clean_air_pace_ms,
    round(tp.avg_pace_ms - cab.clean_air_pace_ms, 0) as traffic_penalty_ms,
    case
        when tp.avg_pace_ms - cab.clean_air_pace_ms < 300 then 'Minimal Impact'
        when tp.avg_pace_ms - cab.clean_air_pace_ms < 800 then 'Moderate Impact'
        when tp.avg_pace_ms - cab.clean_air_pace_ms < 1500 then 'Significant Impact'
        else 'Severe Impact'
    end as traffic_impact_rating
from traffic_performance tp
left join clean_air_baseline cab on tp.driver_id = cab.driver_id