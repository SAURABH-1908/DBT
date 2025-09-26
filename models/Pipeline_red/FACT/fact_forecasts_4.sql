{{
  config(
    materialized = 'incremental',
    unique_key = ['product_code', 'customer_code', 'forecast_date'],
    incremental_strategy = 'merge',
    pre_hook = "SET session_timezone = 'UTC';")
}}

{% if not is_incremental() %}
  -- Initial load
  SELECT 
      product_code,
      customer_code, 
      forecast_date,
      forecast_quantity,
      forecast_sales_value,
      CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
      CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
      CAST(NULL AS TIMESTAMP) AS dss_delete_time
  FROM {{ ref('stage_Forecasts') }}

{% else %}
  -- Incremental MERGE via macro
  {{
    merge_fact_table_enhanced_2(
      target_table = this,
      source_table = ref('stage_Forecasts'),
      join_keys = ['product_code', 'customer_code', 'forecast_date'],
      columns = [
        'product_code',
        'customer_code',
        'forecast_date',
        'forecast_quantity',
        'forecast_sales_value'
      ],
      update_condition = "SRC.forecast_quantity IS DISTINCT FROM TGT.forecast_quantity OR SRC.forecast_sales_value IS DISTINCT FROM TGT.forecast_sales_value",
      incremental_filter_column = 'forecast_date',
      handle_deletes = true,
      delete_column = 'dss_delete_time'
    )
  }}

  -- Return data so dbt model is valid and preview works
  SELECT * FROM {{ this }}

{% endif %}