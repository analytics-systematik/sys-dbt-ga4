{% if var('enable_fivetran_ad_report_mapping', True) %}
    {{ config(materialized='table', unique_key = "surrogate_key", partition_by ={ "field": "date_day", "data_type": "date" }, enabled=true, schema='marts_marketing') }}
{% else %}
    {{ config(enabled=false) }}
{% endif %}

{% set conversions_field = var('conversions_field', 'count_pageviews') %}

with paid_ads as (
    select
        date_day,
        campaign_id,
        platform,
        account_name as account,
        campaign_name as campaign,
        cast(null as {{ dbt.type_string() }}) as session_source,
        cast(null as {{ dbt.type_string() }}) as session_medium,
        clicks,
        impressions,
        spend,
        null as conversions,
        null as revenue,
        null as sessions
    from {{ref('ad_reporting__campaign_report')}} 
),
ga4_sessions_aggregated as (
    select 
        session_start_date as date_day,
        session_utm_id as campaign_id,
        max(case
            when session_source = "facebook" then "meta"
            else session_source
        end) as session_source,
        max(session_medium) as session_medium,
        sum(case when {{ conversions_field }} > 0 then {{ conversions_field }} else 0 end) as conversions,
        sum(case when {{ conversions_field }} > 0 then sum_event_value_in_usd else 0 end) as revenue,
        sum(sessions) as sessions
    from {{ ref('ga4__sessions') }}
    where session_utm_id is not null
    group by 1, 2
),
final as (
    select
        coalesce(paid_ads.date_day, ga4_sessions_aggregated.date_day) as date_day,
        coalesce(paid_ads.campaign_id, ga4_sessions_aggregated.campaign_id) as campaign_id,
        paid_ads.platform,
        paid_ads.account,
        paid_ads.campaign,
        ga4_sessions_aggregated.session_source,
        ga4_sessions_aggregated.session_medium,
        paid_ads.clicks,
        paid_ads.impressions,
        paid_ads.spend,
        ga4_sessions_aggregated.conversions,
        ga4_sessions_aggregated.revenue,
        ga4_sessions_aggregated.sessions
    from paid_ads
    full outer join ga4_sessions_aggregated
        on paid_ads.campaign_id = ga4_sessions_aggregated.campaign_id
        and paid_ads.date_day = ga4_sessions_aggregated.date_day
)

select * from final
