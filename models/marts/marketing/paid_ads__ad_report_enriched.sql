{% if var('enable_fivetran_ad_report_mapping', True) %}
    {{ config(materialized='table', unique_key = "surrogate_key", partition_by ={ "field": "date_day", "data_type": "date" }, enabled=true, schema='marts_marketing') }}
{% else %}
    {{ config(enabled=false) }}
{% endif %}

{% set conversions_field = var('conversions_field', 'count_pageviews') %}
{% set excluded_landing_page_hostnames = var('excluded_landing_page_hostnames', []) %}

with ad_report as (
    select
        date_day,
        platform,
        account_name as account,
        campaign_name as campaign,
        ad_group_name as ad_group,
        ad_name as ad,
        ad_id,
        cast(null as {{ dbt.type_string() }}) as session_source,
        cast(null as {{ dbt.type_string() }}) as session_medium,
        clicks,
        impressions,
        spend
    from {{ref('ad_reporting__ad_report')}} 
),
google_ads_ad_conversions as (
    select
        date_day,
        cast(ad_id as {{ dbt.type_string() }}) as ad_id,
        sum(conversions) as platform_conversions,
        sum(conversions_value) as platform_conversions_value
    from {{ ref('google_ads__ad_report') }}
    group by 1, 2
),
{% if var('enable_meta_platform_conversions', false) %}
meta_ads_ad_conversions as (
    select
        date_day,
        cast(ad_id as {{ dbt.type_string() }}) as ad_id,
        sum(conversions) as platform_conversions,
        sum(conversions_value) as platform_conversions_value
    from {{ ref('facebook_ads__ad_report') }}
    group by 1, 2
),
{% endif %}
paid_ads as (
    select
        ad_report.date_day,
        ad_report.platform,
        ad_report.account,
        ad_report.campaign,
        ad_report.ad_group,
        ad_report.ad,
        ad_report.session_source,
        ad_report.session_medium,
        ad_report.clicks,
        ad_report.impressions,
        ad_report.spend,
        coalesce(
            gac.platform_conversions
            {% if var('enable_meta_platform_conversions', false) %}
            , mac.platform_conversions
            {% endif %}
        ) as platform_conversions,
        coalesce(
            gac.platform_conversions_value
            {% if var('enable_meta_platform_conversions', false) %}
            , mac.platform_conversions_value
            {% endif %}
        ) as platform_conversions_value,
        cast(null as float64) as ga4_session_conversions,
        cast(null as float64) as ga4_session_revenue,
        cast(null as float64) as ga4_last_non_direct_conversions,
        cast(null as float64) as ga4_last_non_direct_revenue
    from ad_report
    left join google_ads_ad_conversions gac
        on ad_report.ad_id = gac.ad_id
        and ad_report.date_day = gac.date_day
    {% if var('enable_meta_platform_conversions', false) %}
    left join meta_ads_ad_conversions mac
        on ad_report.ad_id = mac.ad_id
        and ad_report.date_day = mac.date_day
    {% endif %}
),
ga4_sessions_with_purchases as (
    select 
        session_start_date as date_day,
        cast(null as {{ dbt.type_string() }}) as platform,
        cast(null as {{ dbt.type_string() }}) as account,
        session_campaign as campaign,
        session_ad_group as ad_group,        	
        session_ad_name as ad,
        session_source,
        session_medium,
        null as clicks,
        null as impressions,
        null as spend,
        cast(null as float64) as platform_conversions,
        cast(null as float64) as platform_conversions_value,
        {{ conversions_field }} as ga4_session_conversions,
        sum_event_value_in_usd as ga4_session_revenue,
        cast(null as float64) as ga4_last_non_direct_conversions,
        cast(null as float64) as ga4_last_non_direct_revenue
    from {{ ref('ga4__sessions') }}
    where {{ conversions_field }} > 0
    {% if excluded_landing_page_hostnames | length > 0 %}
        and (landing_page is null 
             or NET.HOST(landing_page) not in (
                 {%- for hostname in excluded_landing_page_hostnames -%}
                     '{{ hostname }}'{% if not loop.last %},{% endif %}
                 {%- endfor -%}
             ))
    {% endif %}
),
ga4_last_non_direct_sessions_with_purchases as (
    select
        fct.session_partition_date as date_day,
        cast(null as {{ dbt.type_string() }}) as platform,
        cast(null as {{ dbt.type_string() }}) as account,
        dim.last_non_direct_campaign as campaign,
        cast(null as {{ dbt.type_string() }}) as ad_group,
        cast(null as {{ dbt.type_string() }}) as ad,
        dim.last_non_direct_source as session_source,
        dim.last_non_direct_medium as session_medium,
        null as clicks,
        null as impressions,
        null as spend,
        cast(null as float64) as platform_conversions,
        cast(null as float64) as platform_conversions_value,
        cast(null as float64) as ga4_session_conversions,
        cast(null as float64) as ga4_session_revenue,
        fct.purchase_count as ga4_last_non_direct_conversions,
        fct.session_partition_sum_event_value_in_usd as ga4_last_non_direct_revenue
    from {{ ref('fct_ga4__sessions_daily') }} fct
    inner join {{ ref('dim_ga4__sessions_daily') }} dim
        on fct.session_partition_key = dim.session_partition_key
    where fct.purchase_count > 0
        and dim.last_non_direct_source != '(direct)'
    {% if excluded_landing_page_hostnames | length > 0 %}
        and (dim.landing_page_location is null 
             or NET.HOST(dim.landing_page_location) not in (
                 {%- for hostname in excluded_landing_page_hostnames -%}
                     '{{ hostname }}'{% if not loop.last %},{% endif %}
                 {%- endfor -%}
             ))
    {% endif %}
),
final as (
    select * from paid_ads
    union all
    select * from ga4_sessions_with_purchases
    union all
    select * from ga4_last_non_direct_sessions_with_purchases
)

select * from final
