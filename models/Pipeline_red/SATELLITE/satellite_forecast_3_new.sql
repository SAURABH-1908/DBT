{{
  config(
    materialized = 'incremental'
  )
}}

{{ automate_dv.sat(
  src_pk            = 'HUB_FORECASTS_HK',
  src_hashdiff      = 'SAT_FORECASTS_HASHDIFF',
  src_payload       = [
    'FORECAST_QUANTITY',
    'FORECAST_SALES_VALUE',
    'FORECAST_DATE',
    'FORECAST_DATE_YEAR',
    'FORECAST_DATE_MONTH'
  ],
  src_extra_columns = [
    'PRODUCT_CODE',
    'CUSTOMER_CODE',
    'PRODUCT_CODE_RANK'
  ],
  src_eff           = 'FORECAST_DATE',
  src_ldts          = 'LDTS',
  src_source        = 'RSRC',
  source_model      = 'dv_stg_forecasts_2_new'
) }}
