{{ 
  config(
    materialized = "view",
    schema = "stg_google_analytics_4"
  ) 
}}


{% set ga4_events =  ['01'] %}
{% for ga4_event in ga4_events %}

    SELECT 
        parse_date('%Y%m%d',event_date) AS event_date,
        * EXCEPT (event_date)

    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source('ga4_' ~ ga4_event, 'events_intraday') }}

    {% else %}
        from {{ source('ga4_' ~ ga4_event, 'events') }}
        where _table_suffix not like '%intraday%'

    {% endif %} 
  
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
