{% macro merge_fact_table_enhanced_2(
    target_table,
    source_table,
    join_keys,
    columns,
    update_condition=none,
    incremental_filter_column=none,
    handle_deletes=false,
    delete_column='dss_delete_time'
) %}

    {%- set update_condition = update_condition or "true" -%}
    {%- set incremental_filter_column = incremental_filter_column or 'dss_update_time' -%}

    {% set merge_sql %}
        MERGE INTO {{ target_table }} AS TGT
        USING (
            -- Current/updated records from source
            SELECT 
                {% for column in columns %}
                {{ column }},
                {% endfor %}
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
                CAST(NULL AS TIMESTAMP) AS {{ delete_column }}
            FROM {{ source_table }}
            {% if is_incremental() %}
            WHERE {{ incremental_filter_column }} > (
                SELECT COALESCE(MAX({{ incremental_filter_column }}), '1900-01-01') 
                FROM {{ target_table }}
                WHERE {{ delete_column }} IS NULL
            )
            {% endif %}

            {% if handle_deletes and is_incremental() %}
            UNION ALL

            -- Records to mark as deleted (missing from source but present in target)
            SELECT 
                {% for column in columns %}
                TGT.{{ column }},
                {% endfor %}
                TGT.dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS {{ delete_column }}
            FROM {{ target_table }} TGT
            LEFT JOIN {{ source_table }} SRC ON 
                {% for key in join_keys %}
                SRC.{{ key }} = TGT.{{ key }}{% if not loop.last %} AND {% endif %}
                {% endfor %}
            WHERE SRC.{{ join_keys[0] }} IS NULL
              AND TGT.{{ delete_column }} IS NULL
              AND TGT.dss_update_time > (
                  SELECT COALESCE(MAX(dss_update_time), '1900-01-01') 
                  FROM {{ target_table }} 
              ) - INTERVAL '7 DAYS'
            {% endif %}
        ) AS SRC
        ON 
            {% for key in join_keys %}
            SRC.{{ key }} = TGT.{{ key }}{% if not loop.last %} AND {% endif %}
            {% endfor %}

        -- Update matched records (simplified condition)
        WHEN MATCHED AND SRC.{{ delete_column }} IS NULL AND (
            {{ update_condition }}
        ) THEN UPDATE SET
            {% for column in columns if column not in join_keys and column not in ['dss_create_time'] %}
            TGT.{{ column }} = SRC.{{ column }}{% if not loop.last %},{% endif %}
            {% endfor %},
            TGT.dss_update_time = SRC.dss_update_time

        -- Soft deletes
        WHEN MATCHED AND SRC.{{ delete_column }} IS NOT NULL THEN
            UPDATE SET
                TGT.dss_update_time = SRC.dss_update_time,
                TGT.{{ delete_column }} = SRC.{{ delete_column }}

        -- Inserts
        WHEN NOT MATCHED AND SRC.{{ delete_column }} IS NULL THEN
            INSERT (
                {% for column in columns %}
                {{ column }},
                {% endfor %}
                dss_create_time,
                dss_update_time,
                {{ delete_column }}
            )
            VALUES (
                {% for column in columns %}
                SRC.{{ column }},
                {% endfor %}
                SRC.dss_create_time,
                SRC.dss_update_time,
                SRC.{{ delete_column }}
            );
    {% endset %}

    {{ log("Executing MERGE statement for incremental update") }}
    {% do run_query(merge_sql) %}
    {{ log("MERGE statement completed successfully") }}

{% endmacro %}