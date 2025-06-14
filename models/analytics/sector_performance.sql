-- models/analytics/mart_sector_performance.sql
-- Purpose: Sector Performance - Analyzing driver performance in specific sectors
-- Identifies driver strengths in different track sections, e.g.
-- (turns vs straights equivalent)
with sector_splits as (
    select
        race_id,
        driver_id,
        lap,
        lap_time_ms,
        -- Estimate sector times (F1 tracks typically have 3 sectors)
        case
            when (lap % 3) = 1 then 'Sector_1'
            when (lap % 3) = 2 then 'Sector_2'
            else 'Sector_3'
        end as estimated_sector,
        lap_time_ms / 3 as estimated_sector_time_ms
    from {{ ref('stg_lap_times') }}
    where lap_time_ms between 60000 and 180000
        and lap > 0
),
sector_performance as (
    select
        race_id,
        driver_id,
        estimated_sector,
        count(*) as sector_attempts,
        avg(estimated_sector_time_ms) as avg_sector_time_ms,
        min(estimated_sector_time_ms) as best_sector_time_ms,
        stddev(estimated_sector_time_ms) as sector_consistency
    from sector_splits
    group by race_id, driver_id, estimated_sector
    having count(*) >= 3
),
driver_sector_strengths as (
    select
        driver_id,
        estimated_sector,
        count(distinct race_id) as races_analyzed,
        avg(avg_sector_time_ms) as overall_avg_sector_ms,
        avg(sector_consistency) as avg_consistency,
        rank() over (
            partition by estimated_sector
            order by avg(avg_sector_time_ms)
        ) as sector_ranking
    from sector_performance
    group by driver_id, estimated_sector
    having count(distinct race_id) >= 2
)

select
    driver_id,
    estimated_sector,
    races_analyzed,
    round(overall_avg_sector_ms, 0) as avg_sector_time_ms,
    round(avg_consistency, 0) as consistency_ms,
    sector_ranking,
    case
        when sector_ranking <= 3 then 'Elite in Sector'
        when sector_ranking <= 6 then 'Strong in Sector'
        when sector_ranking <= 10 then 'Average in Sector'
        else 'Weak in Sector'
    end as sector_strength
from driver_sector_strengths
order by driver_id, estimated_sector