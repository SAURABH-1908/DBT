{{ config(materialized='view') }}

{% set columns = [
    {"name": "PRODUCT_CODE", "data_type": "NUMBER", "is_ldts_column": false, "is_rsrc_column": false, "description": ""},
    {"name": "CUSTOMER_CODE", "data_type": "NUMBER", "is_ldts_column": false, "is_rsrc_column": false, "description": ""},
    {"name": "FORECAST_QUANTITY", "data_type": "NUMBER", "is_ldts_column": false, "is_rsrc_column": false, "description": ""},
    {"name": "FORECAST_SALES_VALUE", "data_type": "NUMBER", "is_ldts_column": false, "is_rsrc_column": false, "description": ""},
    {"name": "FORECAST_DATE", "data_type": "TIMESTAMP", "is_ldts_column": false, "is_rsrc_column": false, "description": ""},
    {"name": "LDTS", "data_type": "TIMESTAMP", "is_ldts_column": true, "is_rsrc_column": false, "description": "The Load Date Timestamp (LDTS) describes when this data first arrived in the Data Warehouse."},
    {"name": "RSRC", "data_type": "VARCHAR", "is_ldts_column": false, "is_rsrc_column": true, "description": "The Record Source (RSRC) describes the source of this data."}
] %}

{% set sources = [
    {
        "source_name": "RAW_SOURCES",
        "table_name": "forecasts",
        "alias": "FORECASTS",
        "columns": columns
    }
] %}

{% set config = {
    "generate_ghost_records": true
} %}

{{ data_vault_stage(columns, sources, config) }}
