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

), 

{% if var('enable_fivetran_ad_report_mapping', True) %}
    ad_mapping as (

        select * from {{ ref('stg_ads__ad_mapping') }}

    ), 
    ad_group_mapping as (

        select * from {{ ref('stg_ads__ad_group_mapping') }}

    ), 
    campaign_mapping as (

        select * from {{ ref('stg_ads__campaign_mapping') }}

    ), 
    final as (

        select
            fct_ga4_sessions.*,
            1 as sessions,
            dim_sessions.*  except(session_start_date, session_start_timestamp, stream_id, session_key, session_number, session_campaign ),
            ad_mapping.ad_name as session_ad_name,
            ad_group_mapping.ad_group_name as session_ad_group,
            campaign_mapping.campaign_name as session_campaign
        from fct_ga4_sessions
        left join dim_sessions 
            on fct_ga4_sessions.session_key = dim_sessions.session_key
            and fct_ga4_sessions.session_start_date = dim_sessions.session_start_date
            and fct_ga4_sessions.session_start_timestamp = dim_sessions.session_start_timestamp
            and fct_ga4_sessions.stream_id = dim_sessions.stream_id
            and fct_ga4_sessions.session_number = dim_sessions.session_number
        left join ad_mapping on ad_mapping.ad_id = dim_sessions.sys_session_ad_id
        left join ad_group_mapping on ad_group_mapping.ad_group_id = dim_sessions.sys_session_ad_group_id
        left join campaign_mapping on campaign_mapping.campaign_id = dim_sessions.session_campaign_id

    )
{% else %}
    final as (

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
{% endif %}

select * from final
