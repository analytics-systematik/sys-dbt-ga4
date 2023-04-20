{% test daily_row_count_within_lower_limit(model, column_name) %}

{{ config(severity = 'warn') }}

WITH daily_row_count AS (
  SELECT
    PARSE_DATE("%Y%m%d", {{ column_name }}) AS event_date,
    COUNT(*) AS row_count
  FROM
    {{ model }}
  GROUP BY
    1
  ORDER BY
    1 
), get_control_limits AS (

SELECT
  event_date,
  row_count,
  CASE
    WHEN row_count BETWEEN ( SELECT AVG(row_count) FROM daily_row_count) - ( SELECT STDDEV_SAMP(row_count) FROM daily_row_count) AND ( SELECT AVG(row_count) FROM daily_row_count) + ( SELECT STDDEV_SAMP(row_count) FROM daily_row_count) THEN 'Within Limits'
    WHEN row_count < ( SELECT AVG(row_count) FROM daily_row_count) - ( SELECT STDDEV_SAMP(row_count) FROM daily_row_count) THEN 'Below Lower Limit'
    WHEN row_count > ( SELECT AVG(row_count) FROM daily_row_count) + ( SELECT STDDEV_SAMP(row_count) FROM daily_row_count) THEN 'Above Upper Limit'
END
  AS control_limits,
FROM
  daily_row_count
GROUP BY
  1, 2
)

SELECT * FROM get_control_limits WHERE control_limits = "Below Lower Limit"

{% endtest %}