{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "session_start_date", "data_type": "date" },
    schema = "google_analytics_4"
  ) 
}}

{% set conversions_field = var('conversions_field', 'count_pageviews') %}

with fct_ga4_sessions as (

    select * from {{ ref('fct_ga4__sessions') }}

), dim_sessions as (

    select * from {{ ref('dim_ga4__sessions') }}

), 
session_first_event as 
(
    select *
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    qualify row_number() over(partition by session_key order by event_timestamp) = 1
),
ga4_sessions as(
    select
            fct_ga4_sessions.*,
            1 as sessions,
            dim_sessions.*  except(session_start_date, session_start_timestamp, stream_id, session_key, session_number, session_campaign, session_source ),
            case
                when session_source = "g" then "google"
                when session_source = "s" then "google"
                when session_source = "d" then "google"
                when session_source = "ytv" then "youtube"
                when session_source = "vp" then "google"
                when session_source = "ig" then "instagram"
                when session_source = "an" then "facebook"
                when session_source = "msg" then "facebook"
                when session_source = "fb" then "facebook"
                else session_source
            end as session_source,
            {{ conversions_field }} as conversions
    from fct_ga4_sessions
    left join dim_sessions 
        on fct_ga4_sessions.session_key = dim_sessions.session_key
        and fct_ga4_sessions.session_start_date = dim_sessions.session_start_date
        and fct_ga4_sessions.session_start_timestamp = dim_sessions.session_start_timestamp
        and fct_ga4_sessions.stream_id = dim_sessions.stream_id
        and fct_ga4_sessions.session_number = dim_sessions.session_number
    left join session_first_event
        on session_first_event.session_key = fct_ga4_sessions.session_key
),
{% if var('query_parameter_extraction', none) != none %}
    add_query_params as (
        select
            *,
            {%- for param in var('query_parameter_extraction') -%}
                {{ get_query_parameter_value( 'landing_page' , param ) }} as {{"session_"+param}}
                {% if not loop.last %},{% endif %}
            {%- endfor -%}
        from ga4_sessions
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
                add_query_params.*,
                ad_mapping.ad_name as session_ad_name,
                ad_group_mapping.ad_group_name as session_ad_group,
                campaign_mapping.campaign_name as session_campaign
            from add_query_params
            left join ad_mapping on ad_mapping.ad_id = add_query_params.session_ad_id
            left join ad_group_mapping on ad_group_mapping.ad_group_id = add_query_params.session_ad_group_id
            left join campaign_mapping on campaign_mapping.campaign_id = add_query_params.session_utm_id

        )
        
        
    {% else %}
        final as (

            select
                *
            from add_query_params
            
        )
    {% endif %}
{% else %}
final as (

    select
        *
    from ga4_sessions
            
)
{% endif %}

select * from final
