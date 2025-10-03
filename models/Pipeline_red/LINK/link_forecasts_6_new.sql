{{
  config(
    materialized = 'incremental'
  )
}}

{{ automate_dv.link(
  src_pk            = 'PRODUCT_CODE',
  src_fk            = [
    'PRODUCT_CODE',
    'CUSTOMER_CODE'
  ],
  src_extra_columns = [
    'FORECAST_QUANTITY',
    'FORECAST_SALES_VALUE',
    'FORECAST_DATE'
  ],
  src_ldts          = 'LDTS',
  src_source        = 'RSRC',
  source_model      = 'dv_stg_forecasts_1'
) }}
