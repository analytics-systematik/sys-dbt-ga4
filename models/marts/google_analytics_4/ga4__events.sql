{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "event_date", "data_type": "date" },
    schema = "marts_marketing",
    materialized = "table"
  ) 
}}


with events as (

  select * from {{ref('stg_ga4__events')}}

),
dim_sessions as(
    
    select * from {{ ref('stg_dim_ga4__sessions') }}
),

final as (

    select 
        events.*,
        dim_sessions.is_first_session,
        session_start_key_page_location,
        session_source,
        session_medium,
        session_campaign,
        session_content,
        session_landing_page,
        session_landing_page_path,
        session_exit_page,
        session_exit_page_path,
    from events
    left join dim_sessions on dim_sessions.session_key = events.session_key
)

select * from final