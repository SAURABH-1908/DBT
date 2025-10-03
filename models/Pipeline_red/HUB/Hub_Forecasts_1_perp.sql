{{
    config(
        materialized='incremental',
        unique_key=hub_name ~ '_hk'
    )
}}

{{ datavault__hub(
    source_relation=ref('dv_stg_forecasts_1'),
    hub_name=this,
    business_key_cols=['PRODUCT_CODE'],
    ldts_col='LDTS',
    record_source="'FORECAST_SYSTEM'",
    hash_col_name='HUB_PRODUCT_HK'
) }}
