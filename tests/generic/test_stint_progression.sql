-- Test that stint numbers increment properly
with stint_gaps as (
    select
        race_id,
        driver_id,
        stint_number,
        lag(stint_number) over (
            partition by race_id, driver_id
            order by stint_number
        ) as prev_stint
    from {{ ref('int_stint_segmentation') }}
    where stint_number > 1
)
select *
from stint_gaps
where stint_number - prev_stint != 1  -- Gaps in stint numbering