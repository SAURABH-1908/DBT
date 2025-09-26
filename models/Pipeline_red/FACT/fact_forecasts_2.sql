{{
  config(
    materialized='incremental',
    unique_key=['product_code', 'customer_code', 'forecast_date']
  )
}}

{{
  merge_fact_table_5(
    target_table=this,
    source_table=ref('stage_Forecasts'),
    join_keys=['product_code', 'customer_code', 'forecast_date'],
    columns=[
      'product_code',
      'customer_code',
      'forecast_date',
      'forecast_quantity',
      'forecast_sales_value'
    ]
  )
}}


