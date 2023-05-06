{% macro cents_to_dollars(column_name) %}
    round({{ column_name }}::float / 100, 2)
{% endmacro %}
