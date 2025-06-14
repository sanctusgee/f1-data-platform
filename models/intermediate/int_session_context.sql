-- models/intermediate/int_session_context.sql
-- Purpose: Add session context to lap data (practice, qualifying, race simulation)

with lap_context as (
    select
        *,
        case
            when lap <= 3 then 'Qualifying Simulation'
            when lap between 4 and 8 then 'Race Simulation'
            else 'Long Run'
        end as session_type,
        case
            when lap = 1 then 'Out Lap'
            when lap <= 5 then 'Early Session'
            when lap <= 10 then 'Mid Session'
            else 'Late Session'
        end as session_phase
    from {{ ref('stg_lap_times') }}
)

select * from lap_context