
{{
  config(
    materialized = 'table'
  )
}}

{{ automate_dv.hub(
     src_pk            = 'CUSTOMER_CODE',
     src_nk            = 'PRODUCT_CODE',
     src_extra_columns = [
       'CUSTOMER_CODE',
       'FORECAST_QUANTITY',
       'FORECAST_SALES_VALUE',
       'FORECAST_DATE'
     ],
     src_ldts          = 'LDTS',
     src_source        = 'RSRC',
     source_model      = 'dv_stg_forecasts_1'
) }}
