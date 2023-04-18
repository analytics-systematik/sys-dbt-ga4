{{ 
  config(
    materialized = "table",
    schema = "stg_google_ads"
  ) 
}}

{% set source_schema = 'src_google_ads' %}
{% set source_table = 'stg_google_ads__ads_history' %}

{% set source_relation = adapter.get_relation(database=target.database, schema=source_schema, identifier=source_table) %}

{% if source_relation %}

with ad_history as (

    select * from {{ source_relation }}

),
ranked_ads as (
  select
    ad_id,
    ad_name,
    updated_at,
    row_number() over (partition by ad_id order by updated_at desc) as row_num
  from
    ad_history
)
select
  ad_id,
  ad_name
from
  ranked_ads
where
  row_num = 1


{% else %}

-- if the source doesn't exist, create an empty model with the same columns as the expected output
select
    null as ad_id,
    null as ad_name
limit 0

{% endif %}