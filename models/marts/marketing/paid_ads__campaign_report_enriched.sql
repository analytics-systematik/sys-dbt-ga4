{% if var('enable_fivetran_ad_report_mapping', True) %}
    {{ config(materialized='table', unique_key = "surrogate_key", partition_by ={ "field": "date_day", "data_type": "date" }, enabled=true, schema='marts_marketing') }}
{% else %}
    {{ config(enabled=false) }}
{% endif %}

{% set conversions_field = var('conversions_field', 'count_pageviews') %}
{% set excluded_landing_page_hostnames = var('excluded_landing_page_hostnames', []) %}

with paid_ads as (
    select
        date_day,
        campaign_id,
        regexp_replace(platform, r'(?i)facebook', 'meta') as platform,
        account_name as account,
        campaign_name as campaign,
        cast(null as {{ dbt.type_string() }}) as session_source,
        cast(null as {{ dbt.type_string() }}) as session_medium,
        clicks,
        impressions,
        spend,
        null as sessions
    from {{ref('ad_reporting__campaign_report')}} 
),
google_ads_conversions as (
    select
        date_day,
        cast(campaign_id as {{ dbt.type_string() }}) as campaign_id,
        sum(conversions) as platform_conversions,
        sum(conversions_value) as platform_conversions_value
    from {{ ref('google_ads__campaign_report') }}
    group by 1, 2
),
{% if var('enable_meta_platform_conversions', false) %}
meta_ads_conversions as (
    select
        date_day,
        cast(campaign_id as {{ dbt.type_string() }}) as campaign_id,
        sum(conversions) as platform_conversions,
        sum(conversions_value) as platform_conversions_value
    from {{ ref('facebook_ads__campaign_report') }}
    group by 1, 2
),
{% endif %}
{% if var('enable_microsoft_platform_conversions', false) %}
microsoft_ads_conversions as (
    select
        date_day,
        cast(campaign_id as {{ dbt.type_string() }}) as campaign_id,
        sum(conversions) as platform_conversions,
        sum(conversions_value) as platform_conversions_value
    from {{ ref('microsoft_ads__campaign_report') }}
    group by 1, 2
),
{% endif %}
ga4_sessions_aggregated as (
    select 
        session_start_date as date_day,
        session_utm_id as campaign_id,
        max(case
            when session_source = "facebook" then "meta"
            else session_source
        end) as session_source,
        max(session_medium) as session_medium,
        sum(case when {{ conversions_field }} > 0 then {{ conversions_field }} else 0 end) as ga4_session_conversions,
        sum(case when {{ conversions_field }} > 0 then sum_event_value_in_usd else 0 end) as ga4_session_revenue,
        sum(sessions) as sessions
    from {{ ref('ga4__sessions') }}
    where 1=1
    {% if excluded_landing_page_hostnames | length > 0 %}
        and (landing_page is null 
             or NET.HOST(landing_page) not in (
                 {%- for hostname in excluded_landing_page_hostnames -%}
                     '{{ hostname }}'{% if not loop.last %},{% endif %}
                 {%- endfor -%}
             ))
    {% endif %}
    group by 1, 2
),
ga4_last_non_direct_sessions as (
    select
        fct.session_partition_date as date_day,
        {{ get_query_parameter_value('dim.landing_page_location', 'utm_id') }} as campaign_id,
        dim.last_non_direct_source,
        dim.last_non_direct_medium,
        fct.session_partition_sum_event_value_in_usd,
        fct.purchase_count
    from {{ ref('fct_ga4__sessions_daily') }} fct
    inner join {{ ref('dim_ga4__sessions_daily') }} dim
        on fct.session_partition_key = dim.session_partition_key
    where dim.last_non_direct_source != '(direct)'
    {% if excluded_landing_page_hostnames | length > 0 %}
        and (dim.landing_page_location is null 
             or NET.HOST(dim.landing_page_location) not in (
                 {%- for hostname in excluded_landing_page_hostnames -%}
                     '{{ hostname }}'{% if not loop.last %},{% endif %}
                 {%- endfor -%}
             ))
    {% endif %}
),
ga4_last_non_direct_conversions as (
    select
        date_day,
        campaign_id,
        sum(case when purchase_count > 0 then purchase_count else 0 end) as ga4_last_non_direct_conversions,
        sum(case when purchase_count > 0 then session_partition_sum_event_value_in_usd else 0 end) as ga4_last_non_direct_revenue
    from ga4_last_non_direct_sessions
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
        coalesce(
            google_ads_conversions.platform_conversions
            {% if var('enable_meta_platform_conversions', false) %}
            , meta_ads_conversions.platform_conversions
            {% endif %}
            {% if var('enable_microsoft_platform_conversions', false) %}
            , microsoft_ads_conversions.platform_conversions
            {% endif %}
        ) as platform_conversions,
        coalesce(
            google_ads_conversions.platform_conversions_value
            {% if var('enable_meta_platform_conversions', false) %}
            , meta_ads_conversions.platform_conversions_value
            {% endif %}
            {% if var('enable_microsoft_platform_conversions', false) %}
            , microsoft_ads_conversions.platform_conversions_value
            {% endif %}
        ) as platform_conversions_value,
        ga4_sessions_aggregated.ga4_session_conversions,
        ga4_sessions_aggregated.ga4_session_revenue,
        ga4_last_non_direct_conversions.ga4_last_non_direct_conversions,
        ga4_last_non_direct_conversions.ga4_last_non_direct_revenue,
        ga4_sessions_aggregated.sessions
    from paid_ads

    left join google_ads_conversions
        on paid_ads.campaign_id = google_ads_conversions.campaign_id
        and paid_ads.date_day = google_ads_conversions.date_day

    {% if var('enable_meta_platform_conversions', false) %}
    left join meta_ads_conversions
        on paid_ads.campaign_id = meta_ads_conversions.campaign_id
        and paid_ads.date_day = meta_ads_conversions.date_day
    {% endif %}

    {% if var('enable_microsoft_platform_conversions', false) %}
    left join microsoft_ads_conversions
        on paid_ads.campaign_id = microsoft_ads_conversions.campaign_id
        and paid_ads.date_day = microsoft_ads_conversions.date_day
    {% endif %}

    left join ga4_sessions_aggregated
        on paid_ads.campaign_id = ga4_sessions_aggregated.campaign_id
        and paid_ads.date_day = ga4_sessions_aggregated.date_day

    left join ga4_last_non_direct_conversions
        on paid_ads.campaign_id = ga4_last_non_direct_conversions.campaign_id
        and paid_ads.date_day = ga4_last_non_direct_conversions.date_day
)

select * from final
