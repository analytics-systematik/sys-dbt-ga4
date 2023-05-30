{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "session_start_date", "data_type": "date" }
  ) 
}}

with events as (

    select * from {{ ref('stg_ga4__events') }}

), dim_sessions as (

    select * from {{ ref('dim_ga4__sessions') }}

)

SELECT 
  events.*,
  sessions.* except (session_key)
FROM events
left join  sessions 
  on events.session_key = sessions.session_key