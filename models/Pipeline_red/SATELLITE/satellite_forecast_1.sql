{{
    config(
        materialized='table'
    )
}}

{% if is_incremental() %}
    {{ satellite_insert_1(
        satellite_table=this,
        source_table=ref('dv_stg_forecasts_1'),
        hashkey_column='PRODUCT_CODE',
        hashdiff_column='FORECAST_SALES_VALUE',
        ldts_column='LDTS',
        rsrc_column='RSRC',
        extra_columns=['CUSTOMER_CODE', 'FORECAST_QUANTITY', 'FORECAST_DATE']
    ) }}
{% else %}
    SELECT 
        PRODUCT_CODE,
        CUSTOMER_CODE,
        FORECAST_QUANTITY,
        FORECAST_SALES_VALUE,
        FORECAST_DATE,
        LDTS,
        RSRC
    FROM {{ ref('dv_stg_forecasts_1') }}
{% endif %}