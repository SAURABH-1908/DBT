{{ config(materialized='table') }}

{{
  merge_fact_table_4(
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



/*
{{
  merge_fact_table_4(
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
}}*/