{{ config(
    materialized='stage_insert',
    truncate=True
) }}


select 
product_code,
customer_code,
forecast_date,
forecast_quantity,
forecast_sales_value,
from {{source('RAW_SOURCES','forecasts')}}

