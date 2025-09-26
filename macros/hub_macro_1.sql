{# macros/datavault/hub.sql #}
{%- macro datavault__hub(
    source_relation,                  
    hub_name,                        
    business_key_cols,                
    ldts_col='ldts',                  
    record_source="'UNKNOWN'",        
    hash_col_name=None                
) -%}
 
{#
Arguments:
- source_relation: a ref()/source() relation (e.g. ref('stg_customer'))
- hub_name: string, e.g. 'hub_customer'
- business_key_cols: list of strings: ['customer_id'] or ['col1','col2']
- ldts_col: load timestamp column name
- record_source: SQL expression for record source (e.g. "'FIVETRAN_stg_customer'")
- hash_col_name: optional override (defaults to hub_name ~ '_hk')
#}
 
{%- if hash_col_name is none -%}
  {%- set hash_col_name = hub_name ~ '_hk' -%}
{%- endif -%}
 
{%- set bk_prepped_list = [] -%}
{%- for col in business_key_cols -%}
  {%- do bk_prepped_list.append("COALESCE(src." ~ col ~ ", '') AS " ~ col) -%}
{%- endfor -%}
 
with src as (
    select * from {{ source_relation }}
),
 
bk_prepped as (
    select distinct
        {{ bk_prepped_list | join(',\n        ') }},
        current_timestamp as {{ ldts_col }},
        {{ record_source }} as record_source
    from src
),
 
incoming as (
    select
        {{ dbt_utils.generate_surrogate_key(business_key_cols) }} as {{ hash_col_name }},
        {{ business_key_cols | join(', ') }},
        {{ ldts_col }},
        record_source
    from bk_prepped
)
 
select *
from incoming inc
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.{{ hash_col_name }} = inc.{{ hash_col_name }}
)
{% endif %}
 
{%- endmacro -%}
 