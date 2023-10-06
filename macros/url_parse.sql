{% macro get_query_parameter_value(url, param) %}
    REGEXP_EXTRACT( {{url}}, r'{{param}}=([^&|\?|#]*)'  )
{% endmacro %}