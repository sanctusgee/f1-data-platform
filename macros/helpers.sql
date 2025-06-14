-- Purpose:
-- Macro to convert lap time strings (like "1:32.456") into total milliseconds for proper numeric analysis.
-- macros/helpers.sql

{% macro DELETE_ME_convert_time_string_to_ms(column_name) %}
(
    case
        when {{ column_name }} like '%:%' then
            split_part({{ column_name }}, ':', 1)::int * 60000 +
            (split_part({{ column_name }}, ':', 2)::float * 1000)::int
        else
            ({{ column_name }}::float * 1000)::int
    end
)
{% endmacro %}

