{{ config(
    materialized='incremental',
    unique_key='PRODUCT_CODE'
) }}

{{ generic_link_insert_5(
    target_relation=this,
    source_relation=ref('dv_stg_forecasts_1'),
    source_columns=['PRODUCT_CODE', 'CUSTOMER_CODE', 'FORECAST_QUANTITY', 'FORECAST_SALES_VALUE', 'FORECAST_DATE', 'LDTS', 'RSRC'],
    link_hashkey='PRODUCT_CODE',
    ldts_alias='LDTS',
    disable_hwm=false,
    timestamp_format="'YYYY-MM-DD HH:MI:SS'",
    beginning_of_all_times="'1900-01-01 00:00:00'"
) }}
