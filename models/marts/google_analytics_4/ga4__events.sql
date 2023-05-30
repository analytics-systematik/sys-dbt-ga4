{{ 
  config(
    unique_key = "surrogate_key",
    partition_by ={ "field": "session_start_date", "data_type": "date" }
  ) 
}}

with events as (

    select * from {{ ref('stg_ga4__events') }}

), sessions as (

    select * from {{ ref('dim_ga4__sessions') }}

)

SELECT 
    events.*,
    sessions.session_start_date,
    sessions.session_start_timestamp,
    sessions.landing_page_path,
    sessions.landing_page,
    sessions.landing_page_hostname,
    sessions.landing_page_referrer,
    sessions.is_first_session,
    sessions.session_source,
    sessions.session_medium,
    sessions.session_campaign,
    sessions.session_content,
    sessions.session_term,
    sessions.session_default_channel_grouping,
    sessions.session_source_category,
    sessions.sys_session_ad_group_id,
    sessions.sys_session_ad_id,
    sessions.sys_session_placement_id,
    sessions.sys_session_landing_page,
    sessions.sys_session_landing_page_path,
    sessions.sys_session_exit_page,
    sessions.sys_session_exit_page_path
FROM events
left join  sessions 
  on events.session_key = sessions.session_key