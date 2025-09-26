{{
  config(
    materialized = 'incremental',
    unique_key = ['originating_currency_code', 'destination_currency_code'],
    transient = false,
    tags = ['dimension'],
    incremental_strategy = 'merge'  
  )
}}

{% if not is_incremental() %}
  /* Initial load */
  SELECT
    originating_currency_code,
    destination_currency_code,
    effective_from_date,
    thru_date,
    exchange_rate,
    exchange_rate_multiplier,
    creating_employee_id,
    created_datetime,
    last_change_employee_id,
    last_change_datetime,
    dss_create_time,
    dss_update_time,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM {{ ref('stg_currency_rate') }}

{% else %}
  /* Custom incremental processing */
  {{ 
    type2_dimension_11(
      source_model=ref('stg_currency_rate'),
      target_model=this,
      business_key=['originating_currency_code', 'destination_currency_code'],
      type2_columns=none,
      non_type2_columns=['effective_from_date', 'thru_date', 'exchange_rate', 'exchange_rate_multiplier',
                        'creating_employee_id', 'created_datetime', 'last_change_employee_id', 'last_change_datetime'],
      current_flag_column='current_flag',
      version_column='version',
      start_date_column='valid_from',
      end_date_column='valid_to',
      create_date_column='dss_create_time',
      update_date_column='dss_update_time'
    ) 
  }}
  
  /* Return nothing to prevent dbt's automatic processing */
  SELECT * 
  FROM {{ this }} 
  
  
{% endif %}