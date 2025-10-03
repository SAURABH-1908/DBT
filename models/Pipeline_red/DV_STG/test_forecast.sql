{{
  config(
    materialized = 'view'
  )
}}

select * 
from {{ source('RAW_SOURCES', 'forecasts') }}
