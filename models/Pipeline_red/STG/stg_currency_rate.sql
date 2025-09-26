{{ config(
    materialized='stage_insert',
    truncate=True
) }}
 
select * from {{ source('jaffle_shop', 'currency_rate_sales') }}