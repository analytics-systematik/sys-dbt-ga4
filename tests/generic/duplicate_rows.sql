{% test duplicate_rows(model) %}

WITH t AS (
    SELECT * FROM {{ model }}
    EXCEPT DISTINCT
    SELECT * FROM {{ model }}
)
SELECT COUNT(*) as num_duplicate_rows FROM t
HAVING COUNT(*) > 0

{% endtest %}