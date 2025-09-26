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
    cast(current_timestamp as timestamp) as dss_create_time,
    cast(current_timestamp as timestamp) as dss_update_time,
    cast(null as timestamp) as dss_delete_time
  from {{ ref('stage_Forecasts') }}

{% else %}
  {{ 
    merge_fact_table_enhanced_2(
      target_table=this,
      source_table=ref('stage_Forecasts'),
      join_keys=['product_code', 'customer_code', 'forecast_date'],
      columns=[
        'product_code',
        'customer_code',
        'forecast_date',
        'forecast_quantity',
        'forecast_sales_value'
      ],
      update_condition="SRC.forecast_quantity IS DISTINCT FROM TGT.forecast_quantity OR SRC.forecast_sales_value IS DISTINCT FROM TGT.forecast_sales_value",
      incremental_filter_column='forecast_date',
      handle_deletes=true
    )
  }}

  select * from {{ this }}

{% endif %}
