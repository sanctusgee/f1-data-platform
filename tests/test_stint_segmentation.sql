-- Purpose:
-- Ensure stints are sequential (i.e., no duplicates or overlaps per race/driver).

select
  race_id,
  driver_id,
  stint_number,
  count(*) as row_count
from {{ ref('int_stint_segmentation') }}
group by race_id, driver_id, stint_number
having count(*) < 1
