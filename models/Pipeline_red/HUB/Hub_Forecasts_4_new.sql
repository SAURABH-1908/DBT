{{
  config(
    materialized = 'incremental'
  )
}}

{{ automate_dv.hub(
     src_pk            = 'HUB_FORECASTS_HK',
     src_nk            = 'CUSTOMER_CODE',
     src_extra_columns = [
       'PRODUCT_CODE',
       'FORECAST_QUANTITY',
       'FORECAST_SALES_VALUE',
       'FORECAST_DATE',
       'FORECAST_DATE_YEAR',
       'FORECAST_DATE_MONTH',
       'PRODUCT_CODE_RANK'
     ],
     src_ldts          = 'LDTS',
     src_source        = 'RSRC',
     source_model      = 'dv_stg_forecasts_2_new'
) }}
