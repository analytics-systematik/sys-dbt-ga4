{% if var('enable_fivetran_ad_report_mapping', True) %}
    {{ config(materialized='table', unique_key = "surrogate_key", partition_by ={ "field": "date_day", "data_type": "date" }, enabled=true, schema='marts_marketing') }}
{% else %}
    {{ config(enabled=false) }}
{% endif %}

{% set conversions_field = var('conversions_field', 'count_pageviews') %}

with paid_ads as (
    select
        date_day,
        platform,
        account_name as account,
        campaign_name as campaign,
        cast(null as {{ dbt.type_string() }}) as session_source,
        cast(null as {{ dbt.type_string() }}) as session_medium,
        clicks,
        impressions,
        spend,
        null as conversions,
        null as revenue
    from {{ref('ad_reporting__campaign_report')}} 
),
ga4_sessions_with_purchases as (
    select 
        session_start_date as date_day,
        cast(null as {{ dbt.type_string() }}) as platform,
        cast(null as {{ dbt.type_string() }}) as account,
        session_campaign as campaign,
        session_source,
        session_medium,
        null as clicks,
        null as impressions,
        null as spend,
        {{ conversions_field }} as conversions,
        sum_event_value_in_usd as revenue
    from {{ ref('ga4__sessions') }}
    where {{ conversions_field }} > 0
),
final as (
    select * from paid_ads
    union all
    select * from ga4_sessions_with_purchases
)

select * from final
