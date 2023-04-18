{{ 
  config(
    materialized = "table",
    schema = "stg_google_analytics_4"
  ) 
}}

with

-- import ctes
events as (

    select * from {{ ref('stg_ga4__events') }}

),
ads as(

    select * from {{ ref('stg_dim_google_ads__ads') }}

), 
ad_groups as(

    select * from {{ ref('stg_dim_google_ads__ad_groups') }}

), 
placements as(

    select * from {{ ref('stg_dim_google_ads__placements') }}

), 

-- logical ctes
first_click_attribution as (

    select 
        session_key,
        key_page_location,
        case
            when contains_substr(key_page_location, 'utm_source') = true then REGEXP_EXTRACT(key_page_location, r'(?i)[?&]utm_source=([^&]+)')
            when contains_substr(key_page_location, 'gclid=') = true then "google"
            when contains_substr(key_page_location, 'fbclid=') = true then "facebook"
            when contains_substr(key_page_location, 'msclkid=') = true then "microsoft"
            when contains_substr(key_page_location, 'li_fat_id=') = true then "linkedin"
            else 
                case 
                    when key_ga_session_number = 1 
                    then traffic_source_source
                    else null
                end
        end as session_source,
        
        case
            when contains_substr(key_page_location, 'utm_medium') = true then REGEXP_EXTRACT(key_page_location, r'(?i)[?&]utm_medium=([^&]+)')
            when contains_substr(key_page_location, 'gclid=') = true then "cpc"
            when contains_substr(key_page_location, 'fbclid=') = true then "cpc"
            when contains_substr(key_page_location, 'msclkid=') = true then "cpc"
            when contains_substr(key_page_location, 'li_fat_id=') = true then "cpc"
            else 
                case 
                    when key_ga_session_number = 1 
                    then traffic_source_medium
                    else null
                end
        end as session_medium,
        
        case
            when contains_substr(key_page_location, 'utm_campaign') = true then REGEXP_EXTRACT(key_page_location, r'(?i)[?&]utm_campaign=([^&]+)')
            when contains_substr(key_page_location, 'gclid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'fbclid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'msclkid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'li_fat_id=') = true then cast(null as string)
            else 
                case
                    when key_ga_session_number = 1
                    then traffic_source_name
                    else null
                end
        end as session_campaign,

        case
            when contains_substr(key_page_location, 'utm_content') = true then REGEXP_EXTRACT(key_page_location, r'(?i)[?&]utm_content=([^&]+)')
            when contains_substr(key_page_location, 'gclid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'fbclid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'msclkid=') = true then cast(null as string)
            when contains_substr(key_page_location, 'li_fat_id=') = true then cast(null as string)
            else 
                case 
                    when key_ga_session_number = 1 
                    then cast(null as string)
                    else cast(null as string)
                end
        end as session_content,

        regexp_extract(key_page_location, r'(?i)[?&]ad_group_id=([^&]+)') as session_ad_group_id,
        regexp_extract(key_page_location, r'(?i)[?&]ad_id=([^&]+)') as session_ad_id,
        regexp_extract(key_page_location, r'(?i)[?&]placement_id=([^&]+)') as session_placement_id,

        regexp_extract(key_page_location, '^([^?]+)') as session_landing_page,
        substr(
            split(key_page_location, '?')[offset(0)],
            length(
                split(split(key_page_location, '?')[offset(0)], '/')[
                    offset(2)
                ]
            )
            + length(split(key_page_location, '/')[offset(0)])
            + 3
        ) as session_landing_page_path,
        regexp_extract(
            exit_page, '^([^?]+)'
        ) as session_exit_page,
        substr(
            split(exit_page, '?')[offset(0)],
            length(
                split(
                    split(exit_page, '?')[offset(0)], '/'
                )[offset(2)]
            )
            + length(split(exit_page, '/')[offset(0)])
            + 3
        ) as session_exit_page_path,
        key_ga_session_number,
        key_ga_session_number = 1 as is_first_session
    from events
    where event_name = "session_start"

),
add_first_click_attribution_to_events as (

    select
        events.*,
        coalesce(first_value(first_click_attribution.key_page_location) over (session_window)) as session_start_key_page_location,
        coalesce(first_value(first_click_attribution.session_source) over (session_window)) as session_source, 
        coalesce(first_value(first_click_attribution.session_medium) over (session_window)) as session_medium, 
        coalesce(first_value(first_click_attribution.session_campaign) over (session_window)) as session_campaign, 
        coalesce(first_value(first_click_attribution.session_ad_id) over (session_window)) as session_ad_id, 
        coalesce(first_value(first_click_attribution.session_ad_group_id) over (session_window)) as session_ad_group_id, 
        coalesce(first_value(first_click_attribution.session_placement_id) over (session_window)) as session_placement_id, 
        coalesce(first_value(first_click_attribution.session_content) over (session_window)) as session_content, 
        coalesce(first_value(first_click_attribution.session_landing_page) over (session_window)) as session_landing_page, 
        coalesce(first_value(first_click_attribution.session_landing_page_path) over (session_window)) as session_landing_page_path, 
        coalesce(first_value(first_click_attribution.session_exit_page) over (session_window)) as session_exit_page, 
        coalesce(first_value(first_click_attribution.session_exit_page_path) over (session_window)) as session_exit_page_path, 
        -- coalesce(first_value(first_click_attribution.key_ga_session_number) over (session_window)) as session_number, 
        coalesce(first_value(first_click_attribution.is_first_session) over (session_window)) as is_first_session, 
    from events
    left join
        first_click_attribution
        on events.session_key = first_click_attribution.session_key
    window session_window as (partition by events.session_key order by event_timestamp rows between unbounded preceding and unbounded following)

),
sessions as (
  
  select 
    session_key,
    -- session_number,
    is_first_session,
    session_start_key_page_location,
    session_source,
    session_medium,
    session_campaign,
    session_content, 	
    session_ad_id,
    session_ad_group_id,
    session_placement_id,
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source,
    session_landing_page,
    session_landing_page_path,
    session_exit_page,
    session_exit_page_path,
    device_category, 
    device_mobile_brand_name, 
    device_mobile_model_name, 
    device_mobile_marketing_name, 
    device_mobile_os_hardware_model, 
    device_operating_system,
    device_operating_system_version, 
    device_advertising_id, 
    device_language, 
    device_is_limited_ad_tracking, 
    device_time_zone_offset_seconds, 
    device_browser,
    device_browser_version,
    geo_region, 
    geo_city, 
    geo_country, 
    geo_continent, 
    geo_sub_continent, 
    geo_metro,
    stream_id,
    platform,
    dense_rank() over(partition by session_key order by event_timestamp) as dense_rank_session_key
  from add_first_click_attribution_to_events
  order by 1

),
final as (

    select 
    distinct 
        sessions.* except(dense_rank_session_key),
        ads.ad_name as session_ad_name,
        ad_groups.ad_group_name as session_ad_group_name,
        placements.placement_name as session_placement_name
    from sessions
    left join ads 
        on cast(ads.ad_id as string) = sessions.session_ad_id
    left join ad_groups
        on cast(ad_groups.ad_group_id as string) = sessions.session_ad_group_id
    left join placements
        on cast(placements.placement_id as string) = sessions.session_placement_id
    
    where dense_rank_session_key = 1
    order by 1

)

select * from final