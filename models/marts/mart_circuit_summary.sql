-- models/marts/mart_circuit_summary.sql
-- Purpose: Circuit-level aggregations to support circuit specialization analytics

with circuit_data as (
    select
        circuit,
        driver_id,
        count(distinct race_id) as races_at_circuit,
        count(*) as total_laps_at_circuit,
        avg(lap_time_ms) as avg_circuit_pace_ms,
        min(lap_time_ms) as best_circuit_pace_ms,
        stddev(lap_time_ms) as circuit_pace_variation,
        -- Track difficulty indicators
        percentile_cont(0.1) within group (order by lap_time_ms) as p10_pace_ms,
        percentile_cont(0.9) within group (order by lap_time_ms) as p90_pace_ms
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 50000 and 300000
        and lap > 0
        and circuit is not null
    group by circuit, driver_id
    having count(*) >= 5  -- Minimum laps for circuit analysis
),
circuit_characteristics as (
    select
        circuit,
        count(distinct driver_id) as drivers_analyzed,
        avg(avg_circuit_pace_ms) as circuit_avg_pace_ms,
        stddev(avg_circuit_pace_ms) as circuit_pace_spread,
        avg(circuit_pace_variation) as avg_circuit_variation,
        case
            when avg(circuit_pace_variation) < 1000 then 'Consistent Track'
            when avg(circuit_pace_variation) < 2000 then 'Moderate Variation'
            else 'High Variation Track'
        end as track_difficulty_profile
    from circuit_data
    group by circuit
    having count(distinct driver_id) >= 3
),
driver_circuit_performance as (
    select
        cd.*,
        cc.circuit_avg_pace_ms,
        cc.track_difficulty_profile,
        (cd.avg_circuit_pace_ms - cc.circuit_avg_pace_ms) as pace_vs_circuit_avg_ms,
        (cd.circuit_pace_variation / cc.avg_circuit_variation) as variation_vs_circuit_avg,
        case
            when (cd.avg_circuit_pace_ms - cc.circuit_avg_pace_ms) < -500 then 'Circuit Specialist'
            when (cd.avg_circuit_pace_ms - cc.circuit_avg_pace_ms) < 0 then 'Above Average'
            when (cd.avg_circuit_pace_ms - cc.circuit_avg_pace_ms) < 500 then 'Average'
            else 'Below Average'
        end as circuit_performance_rating
    from circuit_data cd
    join circuit_characteristics cc on cd.circuit = cc.circuit
)

select
    circuit,
    driver_id,
    races_at_circuit,
    total_laps_at_circuit,
    round(avg_circuit_pace_ms, 0) as avg_circuit_pace_ms,
    round(best_circuit_pace_ms, 0) as best_circuit_pace_ms,
    round(circuit_pace_variation, 0) as circuit_pace_variation_ms,
    round(pace_vs_circuit_avg_ms, 0) as pace_vs_circuit_avg_ms,
    round(variation_vs_circuit_avg, 3) as consistency_vs_circuit_avg,
    circuit_performance_rating,
    track_difficulty_profile
from driver_circuit_performance