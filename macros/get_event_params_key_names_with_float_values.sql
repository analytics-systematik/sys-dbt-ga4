{% macro get_key_names_with_float_values() %}

{% set key_names_query %}
with float_values as(

  select
    event_name,
    params.key as event_parameter_key,
    case
      when params.value.string_value is not null then 'string'
      when params.value.int_value is not null then 'int'
      when params.value.double_value is not null then 'double'
      when params.value.float_value is not null then 'float'
    end as event_parameter_value
  from {{ ref('stg_ga4__events_unioned') }},
    unnest(event_params) as params
  group by
    1,
    2,
    3

)

select 
  distinct(event_parameter_key)
from float_values
where event_parameter_value = "float"   

{% endset %}

{% set results = run_query(key_names_query) %}

{% if execute %}
{# return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}