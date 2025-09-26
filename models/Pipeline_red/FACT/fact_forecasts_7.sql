{{
  config(
    materialized='incremental',
    unique_key=['product_code', 'customer_code', 'forecast_date']
  )
}}

{% if not is_incremental() %}
  select
    product_code,
    customer_code,
    forecast_date,
    forecast_quantity,
    forecast_sales_value,
    cast(current_timestamp as timestamp) as system_create_date,
    cast(current_timestamp as timestamp) as system_update_date
  from {{ ref('stage_Forecasts') }}

{% else %}
  {{ 
    merge_fct_forecasts_dynamic_1(
      target_table=this,
      source_table=ref('stage_Forecasts'),
      join_keys=['product_code', 'customer_code', 'forecast_date'],
      compare_columns=['customer_code', 'forecast_quantity', 'forecast_sales_value', 'forecast_date']
    )
  }}

  select * from {{ this }}

{% endif %}