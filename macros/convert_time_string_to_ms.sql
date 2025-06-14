-- file: macros/convert_time_string_to_ms.sql
-- Purpose: Convert time strings like "1:14.773" to milliseconds

{% macro convert_time_string_to_ms(column_name) %}
    case
        when {{ column_name }} is null or trim({{ column_name }}) = '' then null
        when {{ column_name }} like '%:%' then
            -- Format: "1:37.284" (minutes:seconds.milliseconds)
            (
                split_part({{ column_name }}, ':', 1)::numeric * 60000 +  -- minutes to ms
                split_part({{ column_name }}, ':', 2)::numeric * 1000     -- seconds to ms
            )::integer
        else
            -- Format: "25.208" (just seconds)
            ({{ column_name }}::numeric * 1000)::integer
    end
{% endmacro %}