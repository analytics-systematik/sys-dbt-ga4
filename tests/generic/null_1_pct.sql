{% test null_1_pct(model, column_name) %}

SELECT
  count(*) as num_rows,
  sum(case when {{ column_name }} is null then 1 else 0 end) as num_nulls
FROM {{ model }}
HAVING NOT (num_nulls / num_rows < 0.01)

{% endtest %}