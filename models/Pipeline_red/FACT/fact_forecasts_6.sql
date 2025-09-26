{{
  config(
    materialized='incremental',
    unique_key=['product_code', 'customer_code', 'forecast_date']
  )
}}

{% if not is_incremental() %}
  -- Full refresh: select all records from stage
  select
    product_code,
    customer_code,
    forecast_date,
    forecast_quantity,
    forecast_sales_value,
    cast(current_timestamp as timestamp) as dss_create_time,
    cast(current_timestamp as timestamp) as dss_update_time
  from {{ ref('stage_Forecasts') }}

{% else %}
  -- Incremental load: use merge macro
  {{ 
    merge_fact_table_enhanced_3(
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
      incremental_filter_column='forecast_date'
    )
  }}

  -- Select from target table to maintain materialized view
  select * from {{ this }}

{% endif %}