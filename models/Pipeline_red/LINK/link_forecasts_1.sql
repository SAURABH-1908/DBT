{{ config(
    materialized='incremental',
    unique_key='link_customer_order_hashkey'
) }}

{{ generate_link_sql(
    source_model='dv_stg_forecasts_1',
    parent_models=['hub_customer', 'hub_order'],
    link_hashkey='link_customer_order_hashkey',
    business_keys=['customer_id', 'order_id']
) }}