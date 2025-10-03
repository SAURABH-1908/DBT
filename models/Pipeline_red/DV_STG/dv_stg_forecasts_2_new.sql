{{
  config(
    materialized = 'view'
  )
}}

{% set source_model = 'test_forecast' %}  

{% set derived_columns = {
  'LDTS': 'CURRENT_TIMESTAMP',
  'FORECAST_DATE_YEAR': 'EXTRACT(YEAR FROM FORECAST_DATE)',
  'FORECAST_DATE_MONTH': 'EXTRACT(MONTH FROM FORECAST_DATE)',
  'RSRC' : "!RECORD_SOURCE"
} %}

{% set null_columns = [
  'CUSTOMER_CODE',
  'FORECAST_QUANTITY',
  'FORECAST_SALES_VALUE'
] %}

{% set hashed_columns = {
  'HUB_FORECASTS_HK': ['CUSTOMER_CODE'],
  'SAT_FORECASTS_HASHDIFF': [
    'PRODUCT_CODE',
    'CUSTOMER_CODE',
    'FORECAST_QUANTITY',
    'FORECAST_SALES_VALUE',
    'FORECAST_DATE'
  ],
  'LINK_FORECASTS_HK': ['PRODUCT_CODE', 'CUSTOMER_CODE']
} %}

{% set ranked_columns = {
  'PRODUCT_CODE_RANK': {
    'partition_by': ['PRODUCT_CODE'],
    'order_by': ['LDTS desc']
  }
} %}

{{ automate_dv.stage(
    source_model           = source_model,
    include_source_columns = true,
    derived_columns        = derived_columns,
    null_columns           = null_columns,
    hashed_columns         = hashed_columns,
    ranked_columns         = ranked_columns
) }}


