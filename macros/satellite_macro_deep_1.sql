{% macro satellite_insert_1(satellite_table, source_table, hashkey_column, hashdiff_column, ldts_column='LDTS', rsrc_column='RSRC', extra_columns=none, disable_hwm=false) %}

{% set extra_columns = extra_columns or [] %}
{% set all_source_columns = [hashkey_column, hashdiff_column, ldts_column, rsrc_column] + extra_columns %}

WITH latest_entries_in_sat AS (
    SELECT 
        {{ hashkey_column }},
        {{ hashdiff_column }}
    FROM {{ satellite_table }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ hashkey_column }}
        ORDER BY {{ ldts_column }} DESC
    ) = 1
),

deduplicated_numbered_source AS (
    SELECT
        {% for column in all_source_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %},
        ROW_NUMBER() OVER (
            PARTITION BY {{ hashkey_column }}
            ORDER BY {{ ldts_column }}
        ) as rn
    FROM {{ source_table }}
    
    {% if not disable_hwm %}
    WHERE {{ ldts_column }} > (
        SELECT 
            COALESCE(
                MAX({{ ldts_column }}), 
                '0001-01-01 00:00:01'::timestamp
            )
        FROM {{ satellite_table }}
        WHERE {{ ldts_column }} != '8888-12-31 23:59:59'::timestamp
    )
    {% endif %}
    
    QUALIFY CASE
        WHEN {{ hashdiff_column }} = LAG({{ hashdiff_column }}) OVER (
            PARTITION BY {{ hashkey_column }}
            ORDER BY {{ ldts_column }}
        ) THEN FALSE
        ELSE TRUE
    END
)

SELECT DISTINCT
    {% for column in all_source_columns %}
    {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM deduplicated_numbered_source
WHERE NOT EXISTS (
    SELECT 1
    FROM latest_entries_in_sat
    WHERE deduplicated_numbered_source.{{ hashdiff_column }} = latest_entries_in_sat.{{ hashdiff_column }}
      AND deduplicated_numbered_source.{{ hashkey_column }} = latest_entries_in_sat.{{ hashkey_column }}
      AND deduplicated_numbered_source.rn = 1
)

{% endmacro %}