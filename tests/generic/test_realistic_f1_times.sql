-- Test that lap times are realistic for F1
select *
from {{ ref('stg_lap_times') }}
where lap_time_ms < 60000  -- Too fast (< 1 minute)
   or lap_time_ms > 180000 -- Too slow (> 3 minutes)