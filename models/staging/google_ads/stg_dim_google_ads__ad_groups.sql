{{ 
  config(
    materialized = "table",
    schema = "stg_google_ads"
  ) 
}}

{% set source_schema = 'src_google_ads' %}
{% set source_table = 'stg_google_ads__ad_group_history' %}

{% set source_relation = adapter.get_relation(database=target.database, schema=source_schema, identifier=source_table) %}

{% if source_relation %}

with ad_group_history as (

    select * from {{ source_relation }}

),
ranked_ads as (
  select
    ad_group_id,
    ad_group_name,
    updated_at,
    row_number() over (partition by ad_group_id order by updated_at desc) as row_num
  from
    ad_group_history
)
select
  ad_group_id,
  ad_group_name
from
  ranked_ads
where
  row_num = 1


{% else %}

-- if the source doesn't exist, create an empty model with the same columns as the expected output
select
    null as ad_group_id,
    null as ad_group_name
limit 0

{% endif %}