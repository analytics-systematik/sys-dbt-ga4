{% if var('enable_fivetran_ad_report_mapping', True) %}
    {{ config(enabled=true, schema='stg_google_analytics_4') }}
{% else %}
    {{ config(enabled=false) }}
{% endif %}

with ad_reporting__ad_report as (
    select 
        distinct
        campaign_id,
        campaign_name
    from {{ ref('ad_reporting__ad_report') }}
)

select * from ad_reporting__ad_report