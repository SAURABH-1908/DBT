{{
  config(
    materialized='table',
    unique_key='dim_product_key',
    incremental_strategy='merge',
    transient=False,
    tags=['dimension']
  )
}}

{% if not is_incremental() %}

-- Initial load with SCD Type 2 columns and surrogate key
select
    {{ dbt_utils.generate_surrogate_key(['ps.product_id', 'pm.sales_source']) }} as dim_product_key,
    ps.product_id,
    ps.product_code,
    ps.product_name,
    ps.product_description,
    ps.product_line_code,
    pm.sales_source,
    ps.product_line_description,
    ps.product_group_code,
    ps.product_group_description,
    ps.product_sub_group_code,
    ps.product_sub_group_description,
    ps.barcode_value,
    ps.vendor_id,
    ps.dimension_uom,
    ps.dimension_1,
    ps.dimension_2,
    ps.dimension_3,
    ps.volume_uom,
    ps.volume,
    ps.weight_uom,
    ps.weight,
    ps.auto_reorder_flag,
    ps.auto_reorder_amount,
    ps.creating_employee_id,
    ps.created_datetime,
    ps.last_change_employee_id,
    ps.last_change_datetime,
    current_timestamp() as dss_create_time,
    current_timestamp() as dss_update_time
from {{ ref('stage_product_sales') }} ps
left join {{ ref('stage_product_mrg') }} pm
    on ps.product_id = pm.product_id

{% else %}

-- SCD Type 2 incremental logic
{{
  type2_dimension_11(
    source_model=ref('stage_product_sales'),
    target_model=this,
    business_key='product_id,sales_source',
    type2_columns='vendor_id,weight,volume',
    non_type2_columns='product_code,product_name,product_description,product_line_code,product_line_description,product_group_code,product_group_description,product_sub_group_code,product_sub_group_description,barcode_value,dimension_uom,dimension_1,dimension_2,dimension_3,volume_uom,weight_uom,auto_reorder_flag,auto_reorder_amount,creating_employee_id,created_datetime,last_change_employee_id,last_change_datetime,dss_create_time,dss_update_time',
    current_flag_column='current_flag',
    version_column='version',
    start_date_column='valid_from',
    end_date_column='valid_to',
    create_date_column='created_at',
    update_date_column='updated_at'
  )
}}

-- Prevent dbt default incremental merge
select * from {{ this }}

{% endif %}