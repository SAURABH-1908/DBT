select
    product_code,
    customer_code,
    forecast_date,
    forecast_quantity,
    forecast_sales_value,
    cast(current_timestamp as timestamp) as dss_create_time,
    cast(current_timestamp as timestamp) as dss_update_time
  from {{ ref('stage_Forecasts') }}