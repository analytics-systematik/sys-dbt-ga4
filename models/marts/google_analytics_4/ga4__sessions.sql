{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "session_start_date", "data_type": "date" }
  ) 
}}

with fct_ga4_sessions as (

    select * from {{ ref('fct_ga4__sessions') }}

), dim_sessions as (

    select * from {{ ref('dim_ga4__sessions') }}

), final as (

    select
        fct_ga4_sessions.*,
        1 as sessions,
        dim_sessions.*  except(session_start_date, session_start_timestamp, stream_id, session_key, session_number )
    from fct_ga4_sessions
    left join dim_sessions 
        on fct_ga4_sessions.session_key = dim_sessions.session_key
        and fct_ga4_sessions.session_start_date = dim_sessions.session_start_date
        and fct_ga4_sessions.session_start_timestamp = dim_sessions.session_start_timestamp
        and fct_ga4_sessions.stream_id = dim_sessions.stream_id
        and fct_ga4_sessions.session_number = dim_sessions.session_number

)

select * from final
