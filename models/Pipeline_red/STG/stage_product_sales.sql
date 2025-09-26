{{ config(
    materialized='stage_insert',
    truncate=True
) }}

with product as (
    select * from {{ source('RAW_SOURCES','product') }}
),

product_group as (
    select * from {{ source('RAW_SOURCES','product_group') }}
),

product_sub_group as (
    select * from {{ source('RAW_SOURCES','product_sub_group') }}
),

product_line as (
    select * from {{ source('RAW_SOURCES','product_line') }}
),

final as (
    select
        product.product_id,
        product.product_code,
        product.product_name,
        product.product_description,
        product.product_line_code,
        product_line.product_line_description,
        0 as sales_source,
        product.product_group_code,
        product_group.product_group_description,
        product.product_sub_group_code,
        product_sub_group.product_sub_group_description,
        product.barcode_value,
        product.vendor_id,
        product.dimension_uom,
        product.dimension_1,
        product.dimension_2,
        product.dimension_3,
        product.volume_uom,
        product.volume,
        product.weight_uom,
        product.weight,
        product.auto_reorder_flag,
        product.auto_reorder_amount,
        product.creating_employee_id,
        product.created_datetime,
        product.last_change_employee_id,
        product.last_change_datetime,
        CURRENT_TIMESTAMP() as dss_create_time,   
        CURRENT_TIMESTAMP() as dss_update_time 
    from product
    left join product_line 
        on product.product_line_code = product_line.product_line_code
    left join product_group 
        on product.product_group_code = product_group.product_group_code
    left join product_sub_group 
        on product.product_sub_group_code = product_sub_group.product_sub_group_code
)

select * from final
