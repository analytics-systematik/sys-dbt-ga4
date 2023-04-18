{% macro column_exists(table, column) %}
    {% set columns = adapter.get_columns_in_relation(table) %}
    {% set column_names = columns | map(attribute='name') | list %}
    {% if column in column_names %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}