{{
    config(
        materialized="view",
        schema = "stg_google_analytics_4"
    )
}}

-- Extract relevant fields and create custom fields for the events_blend CTE
with
    events_blend as (

        select
            -- User and event identifiers
            user_id,
            user_pseudo_id,
            stream_id,
            event_bundle_sequence_id,
            device.vendor_id as device_vendor_id,
            device.advertising_id as device_advertising_id,
            items.item_id,
            items.location_id,
            items.item_list_id,
            items.promotion_id,

            -- Dimensions and fields related to event, geo, device, items, ecommerce, user_ltv, privacy_info, app_info, and traffic_source
            event_name,
            split(
                split(
                    (
                        select value.string_value
                        from unnest(event_params)
                        where key = 'page_location'
                    ),
                    '?'
                )[offset(0)],
                '/'
            )[offset(2)] as hostname,
            geo.region as geo_region,
            geo.city as geo_city,
            geo.country as geo_country,
            geo.continent as geo_continent,
            geo.sub_continent as geo_sub_continent,
            geo.metro as geo_metro,
            device.category as device_category,
            device.mobile_brand_name as device_mobile_brand_name,
            device.mobile_model_name as device_mobile_model_name,
            device.mobile_marketing_name as device_mobile_marketing_name,
            device.mobile_os_hardware_model as device_mobile_os_hardware_model,
            device.operating_system as device_operating_system,
            device.operating_system_version as device_operating_system_version,
            device.language as device_language,
            device.is_limited_ad_tracking as device_is_limited_ad_tracking,
            device.time_zone_offset_seconds as device_time_zone_offset_seconds,
            device.browser as device_browser,
            device.browser_version as device_browser_version,
            items.item_name,
            items.item_brand,
            items.item_variant,
            items.item_category,
            items.item_category2,
            items.item_category3,
            items.item_category4,
            items.item_category5,
            items.price_in_usd,
            items.price,
            items.quantity,
            items.item_revenue_in_usd,
            items.item_revenue,
            items.item_refund_in_usd,
            items.item_refund,
            items.coupon,
            items.affiliation,
            items.item_list_name,
            items.item_list_index,
            items.promotion_name,
            items.creative_name,
            items.creative_slot,
            ecommerce.total_item_quantity as ecommerce_total_item_quantity,
            ecommerce.purchase_revenue_in_usd as ecommerce_purchase_revenue_in_usd,
            ecommerce.purchase_revenue as ecommerce_purchase_revenue,
            ecommerce.refund_value_in_usd as ecommerce_refund_value_in_usd,
            ecommerce.refund_value as ecommerce_refund_value,
            ecommerce.shipping_value_in_usd as ecommerce_shipping_value_in_usd,
            ecommerce.shipping_value as ecommerce_shipping_value,
            ecommerce.tax_value_in_usd as ecommerce_tax_value_in_usd,
            ecommerce.tax_value as ecommerce_tax_value,
            ecommerce.unique_items as ecommerce_unique_items,
            ecommerce.transaction_id as ecommerce_transaction_id,
            user_ltv.revenue as user_ltv_revenue,
            user_ltv.currency as user_ltv_currency,
            event_value_in_usd,
            privacy_info.analytics_storage as privacy_info_analytics_storage,
            privacy_info.ads_storage as privacy_info_ads_storage,
            privacy_info.uses_transient_token as privacy_info_uses_transient_token,
            app_info.id as app_info_id,
            app_info.version as app_info_version,
            app_info.install_store as app_info_install_store,
            app_info.firebase_app_id as app_info_firebase_app_id,
            app_info.install_source as app_info_install_source,
            traffic_source.name as traffic_source_name,
            traffic_source.medium as traffic_source_medium,
            traffic_source.source as traffic_source_source,
            platform,
            event_dimensions.hostname as event_dimensions_hostname,
            first_value(
                (
                    select value.string_value
                    from unnest(event_params)
                    where event_name = 'page_view' and key = 'page_location'
                )
            ) over (
                partition by
                    user_pseudo_id,
                    (
                        select value.int_value
                        from unnest(event_params)
                        where event_name = 'page_view' and key = 'ga_session_id'
                    )
                order by events_tables.event_timestamp desc
            ) as exit_page,
            
            -- date/times
            events_tables.event_date,
            timestamp_micros(events_tables.event_timestamp) as event_timestamp,
            timestamp_micros(event_previous_timestamp) as event_previous_timestamp,
            timestamp_micros(event_server_timestamp_offset) as event_server_timestamp_offset,
            timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,

            -- event key_names
            {% for key_names in get_key_names_with_int_values() %}
                (
                    select value.int_value as even_params_value
                    from unnest(event_params)
                    where key = "{{key_names}}"
                ) as key_{{ key_names }},
            {% endfor %}

            {% for key_names in get_key_names_with_string_values() %}
                (
                    select value.string_value as even_params_value
                    from unnest(event_params)
                    where key = "{{key_names}}"
                ) as key_{{ key_names }},
            {% endfor %}

            {% for key_names in get_key_names_with_int_values_like__id() %}
                (
                    select cast(value.int_value as string) as even_params_value
                    from unnest(event_params)
                    where key = "{{key_names}}"
                ) as key_{{ key_names }},
            {% endfor %}

            {% for key_names in get_key_names_with_float_values() %}
                (
                    select value.float_value as even_params_value
                    from unnest(event_params)
                    where key = "{{key_names}}"
                ) as key_{{ key_names }},
            {% endfor %}

            {% for key_names in get_key_names_with_double_values() %}
                (
                    select value.double_value as even_params_value
                    from unnest(event_params)
                    where key = "{{key_names}}"
                ) as key_{{ key_names }},
            {% endfor %}

        from {{ ref("stg_ga4__events_unioned") }} events_tables
        left join unnest(items) as items

    ),

    -- Generate session_key from stream_id, user_pseudo_id, and key_ga_session_id
    add_session_key as (

        select 
            *,
            to_base64(md5(concat(stream_id, user_pseudo_id, cast(key_ga_session_id as string)))) as session_key
        from events_blend

    ),

    -- Extract the key_page_location_page_path field from key_page_location
    add_key_page_location_page_path as (

        select *,
            substr(
                split(key_page_location, '?')[offset(0)],
                length(split(split(key_page_location, '?')[offset(0)], '/')[offset(2)])
                + length(split(key_page_location, '/')[offset(0)])
                + 3
            ) as key_page_location_page_path
        from add_session_key
    )

-- Final selection of all columns from the add_key_page_location_page_path CTE
select *
from add_key_page_location_page_path