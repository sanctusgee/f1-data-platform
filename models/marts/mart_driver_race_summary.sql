-- Purpose:
-- Top-level summary for each driver’s race: average pace, stint count, degradation.

with stint_summary as (
    select * from {{ ref('mart_stint_summary') }}
),
degradation_summary as (
    select * from {{ ref('mart_stint_degradation_summary') }}
),
race_metrics as (
    select
        s.race_id,
        s.driver_id,
        count(distinct s.stint_number) as stint_count,
        max(s.total_laps) as longest_stint_laps,
        avg(s.avg_lap_time_ms) as avg_lap_time_ms,
        avg(d.avg_pace_trend_ms) as avg_degradation_rate
    from stint_summary s
    left join degradation_summary d
        on s.race_id = d.race_id
        and s.driver_id = d.driver_id
        and s.stint_number = d.stint_number
    group by s.race_id, s.driver_id
)

select * from race_metrics
