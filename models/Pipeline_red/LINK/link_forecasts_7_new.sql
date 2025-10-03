{{
  config(
    materialized = 'incremental'
  )
}}

{{ automate_dv.link(
  src_pk            = 'LINK_FORECASTS_HK',
  src_fk            = [
    'HUB_FORECASTS_HK',
    'PRODUCT_CODE',
    'CUSTOMER_CODE'
  ],
  src_ldts          = 'LDTS',
  src_extra_columns = [
    'FORECAST_QUANTITY',
    'FORECAST_SALES_VALUE',
    'FORECAST_DATE',
    'FORECAST_DATE_YEAR',
    'FORECAST_DATE_MONTH',
    'PRODUCT_CODE_RANK'
  ],
  src_source        = 'RSRC',
  source_model      = 'dv_stg_forecasts_2_new'
) }}
