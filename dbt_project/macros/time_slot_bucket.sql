{% macro time_slot_bucket(hour_col) %}
    case
        when {{ hour_col }} between 8 and 11 then 'morning'
        when {{ hour_col }} between 12 and 15 then 'lunch'
        when {{ hour_col }} between 16 and 19 then 'evening'
        when {{ hour_col }} between 20 and 23 then 'dinner'
        else 'late_night'
    end
{% endmacro %}
