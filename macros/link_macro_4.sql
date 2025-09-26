{% macro generic_link_insert_4(
    target_relation,
    source_relation,
    source_columns,
    link_hashkey,
    ldts_alias,
    disable_hwm=false
) %}
    
    {% set column_list = source_columns | join(', ') %}
    
    INSERT INTO {{ target_relation }}
    WITH incoming AS (
        SELECT DISTINCT {{ column_list }}
        FROM {{ source_relation }}
        {% if not disable_hwm %}
        WHERE {{ ldts_alias }} > (
            SELECT COALESCE(MAX({{ ldts_alias }}), '1900-01-01 00:00:00'::TIMESTAMP)
            FROM {{ target_relation }}
        )
        {% endif %}
    ),
    new_records AS (
        SELECT {{ column_list }}
        FROM incoming SRC
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ target_relation }} TGT
            WHERE SRC.{{ link_hashkey }} = TGT.{{ link_hashkey }}
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {{ link_hashkey }}
            ORDER BY {{ ldts_alias }}
        ) = 1
    )
    SELECT {{ column_list }}
    FROM new_records SRC

{% endmacro %}