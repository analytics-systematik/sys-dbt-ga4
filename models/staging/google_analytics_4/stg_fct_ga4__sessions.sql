{{
    config(
        materialized="table",
        unique_key="surrogate_key",
        partition_by={"field": "session_start_date", "data_type": "date"},
        schema = "stg_google_analytics_4"
    )
}}

{% set key_transaction_id_exists = column_exists(ref("stg_ga4__events"), "key_transaction_id") %}

with

    -- import ctes
    events as (
    
        select * from {{ ref("stg_ga4__events") }}

    ),

    -- logical ctes
    new_users as (

        select 
            session_key, 
            1 as new_user
        from events
        where event_name = "first_visit"
        group by 1,2

    ),
    session_revenue as (

        select 
            session_key,
            ecommerce_purchase_revenue as session_revenue, 
            row_number() over (partition by session_key) as row_num
        from {{ ref("stg_ga4__events") }}
        where ecommerce_purchase_revenue is not null
    ),
    session_aggregates as (

        select
            events.session_key,
            new_users.new_user  as new_users,
            session_revenue.session_revenue as session_ecommerce_purchase_revenue,
            min(event_date) as session_start_date,
            sum(case when event_name = "page_view" then 1 else 0 end) AS page_views,
            sum(key_session_engaged) as engaged_sessions,
            sum(key_engagement_time_msec) as engaged_time_msec,
            {% if key_transaction_id_exists %}
                count(distinct key_transaction_id) as purchases,
            {% else %}
                0 as purchases,
            {% endif %}
            1 AS sessions
        from events
        left join new_users on new_users.session_key = events.session_key
        left join session_revenue on session_revenue.session_key = events.session_key and session_revenue.row_num = 1
        group by 1,2,3

    )

select *
from session_aggregates
