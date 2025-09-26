{{
  config(
    materialized='incremental',
    unique_key=['product_code', 'customer_code', 'forecast_date']
  )
}}

SELECT
  product_code,
  customer_code,
  forecast_date,
  forecast_quantity,
  forecast_sales_value,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS system_create_date,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS system_update_date
FROM {{ ref('stage_Forecasts') }}

{% if is_incremental() %}
WHERE forecast_date > (SELECT MAX(forecast_date) FROM {{ this }})
{% endif %}