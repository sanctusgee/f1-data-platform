-- models/analytics/mart_circuit_specialization.sql
-- Purpose: Circuit Specialization - Identifying drivers who excel at specific circuits
-- This model analyzes driver performance at specific circuits to identify specialization patterns.

with circuit_performance as (
    select
        l.driver_id,
        l.circuit,
        count(distinct l.race_id) as races_at_circuit,
        avg(l.lap_time_ms) as avg_lap_time_ms,
        min(l.lap_time_ms) as best_lap_ms,
        stddev(l.lap_time_ms) as pace_variation
    from {{ ref('stg_lap_times') }} l
    where l.lap_time_ms between 60000 and 180000
        and l.lap > 0
    group by l.driver_id, l.circuit
    having count(*) >= 5  -- Minimum laps for analysis
),
driver_averages as (
    select
        driver_id,
        avg(avg_lap_time_ms) as overall_avg_pace,
        avg(pace_variation) as overall_variation
    from circuit_performance
    group by driver_id
),
specialization_analysis as (
    select
        cp.driver_id,
        cp.circuit,
        cp.races_at_circuit,
        cp.avg_lap_time_ms,
        cp.best_lap_ms,
        da.overall_avg_pace,
        (cp.avg_lap_time_ms - da.overall_avg_pace) as pace_delta_from_average,
        (cp.pace_variation / da.overall_variation) as variation_ratio
    from circuit_performance cp
    join driver_averages da on cp.driver_id = da.driver_id
)

select
    driver_id,
    circuit,
    races_at_circuit,
    round(avg_lap_time_ms, 0) as avg_lap_time_ms,
    round(pace_delta_from_average, 0) as pace_vs_personal_avg_ms,
    round(variation_ratio, 3) as consistency_vs_average,
    case
        when pace_delta_from_average < -500 then 'Circuit Specialist'
        when pace_delta_from_average < 0 then 'Above Average'
        when pace_delta_from_average < 500 then 'Average Performance'
        else 'Below Average'
    end as circuit_rating
from specialization_analysis
order by driver_id, pace_delta_from_average