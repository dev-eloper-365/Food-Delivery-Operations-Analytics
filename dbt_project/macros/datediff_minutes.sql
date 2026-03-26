{% macro datediff_minutes(start_col, end_col) %}
    (epoch({{ end_col }}) - epoch({{ start_col }})) / 60
{% endmacro %}
