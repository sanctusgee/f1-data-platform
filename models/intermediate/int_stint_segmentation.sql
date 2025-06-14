-- file: int_stint_segmentation.sql
-- Purpose:
-- Detect driver stints (sequences between pit stops), assign stint numbers.

with laps as (
    select * from {{ ref('stg_lap_times') }}
),
pits as (
    select * from {{ ref('stg_pit_stops') }}
),
joined as (
    select
        l.*,
        case
            when p.pit_duration_ms is not null and p.pit_duration_ms > 0
            then 1
            else 0
        end as pitted
    from laps l
    left join pits p
        on l.race_id = p.race_id
        and l.driver_id = p.driver_id
        and l.lap = p.lap
),
cleaned as (
    select *
    from joined
    where
        lap_time_ms is not null
        and lap is not null
        and lap_time_ms > 0
        and lap > 0
),
with_flags as (
    select *,
        sum(pitted) over (
            partition by race_id, driver_id
            order by lap
            rows unbounded preceding
        ) + 1 as stint_number
    from cleaned
)

select *
from with_flags