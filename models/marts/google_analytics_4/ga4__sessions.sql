{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "session_start_date", "data_type": "date" },
    schema = "marts_marketing",
    materialized = "table"
  ) 
}}



with fct_ga4_sessions as (

    select * from {{ ref('stg_fct_ga4__sessions') }}

), dim_sessions as (

    select * from {{ ref('stg_dim_ga4__sessions') }}

), final as (

    select
        fct_ga4_sessions.* except(session_key),
        dim_sessions.*  except(session_key)
    from fct_ga4_sessions
    left join dim_sessions on fct_ga4_sessions.session_key = dim_sessions.session_key  

)

select * from final
