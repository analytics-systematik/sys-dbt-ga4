{% test ga4_duplicate_rows_1_pct(model) %}

WITH get_columns AS (SELECT 
  ARRAY_TO_STRING(ARRAY(SELECT key FROM UNNEST(event_params)), ',') as key,
  ARRAY_TO_STRING(ARRAY(SELECT value.string_value FROM UNNEST(event_params)), ',') as string_value,
  ARRAY_TO_STRING(ARRAY(SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params)), ',') as int_value,
  ARRAY_TO_STRING(ARRAY(SELECT CAST(value.float_value AS STRING) FROM UNNEST(event_params)), ',') as float_value,
  event_name,
  event_timestamp
FROM {{ model }} )

SELECT 
  COUNT(DISTINCT(CONCAT(event_name,event_timestamp,key,string_value,int_value,float_value))) AS unique_rows,
  (SELECT COUNT(*) FROM {{ model }}) AS num_rows
FROM get_columns
HAVING (unique_rows / num_rows < 0.01)

{% endtest %}