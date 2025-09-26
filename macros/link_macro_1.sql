{% macro generate_link_sql(source_model, parent_models, link_hashkey, business_keys) %}

{% set ldts_alias = var('ldts_alias', 'load_timestamp') %}
{% set beginning_of_all_times = var('beginning_of_all_times', '1900-01-01 00:00:00') %}

WITH incoming AS (
    SELECT DISTINCT
        {{ link_hashkey }},
        {% for key in business_keys %}
        {{ key }}
        {%- if not loop.last -%}, {% endif %}
        {% endfor %},
        {{ ldts_alias }},
        '{{ source_model }}' as record_source
    FROM {{ ref(source_model) }}
    {% if is_incremental() %}
    WHERE {{ ldts_alias }} > (
        SELECT COALESCE(MAX({{ ldts_alias }}), 
               TO_TIMESTAMP('{{ beginning_of_all_times }}', 'YYYY-MM-DD HH24:MI:SS.FF')
        FROM {{ this }}
    )
    {% endif %}
),

new_records AS (
    SELECT
        {{ link_hashkey }},
        {% for key in business_keys %}
        {{ key }}
        {%- if not loop.last -%}, {% endif %}
        {% endfor %},
        {{ ldts_alias }},
        record_source
    FROM incoming SRC
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} TGT
        WHERE SRC.{{ link_hashkey }} = TGT.{{ link_hashkey }}
    )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ link_hashkey }} 
        ORDER BY {{ ldts_alias }}
    ) = 1
)

SELECT *
FROM new_records

{% endmacro %}
