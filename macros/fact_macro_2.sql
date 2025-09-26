{% macro merge_fact_table_5(
    target_table,
    source_table,
    join_keys,
    columns,
    update_condition=none
) %}

    {%- set update_condition = update_condition or "true" -%}
    
    {% if is_incremental() %}
        MERGE INTO {{ target_table }} AS "TGT"
        USING (
            SELECT 
                {% for column in columns %}
                {{ column }},
                {% endfor %}
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time
            FROM {{ source_table }}
        ) AS "SRC"
        ON 
            {% for key in join_keys %}
            SRC.{{ key }} = TGT.{{ key }} {% if not loop.last %}AND{% endif %}
            {% endfor %}
        
        WHEN MATCHED AND ({{ update_condition }}) THEN
            UPDATE SET
                {% for column in columns if column not in join_keys %}
                TGT.{{ column }} = SRC.{{ column }}{% if not loop.last %},{% endif %}
                {% endfor %}
                TGT.dss_update_time = SRC.dss_update_time
        
        WHEN NOT MATCHED THEN
            INSERT (
                {% for column in columns %}
                {{ column }},
                {% endfor %}
                dss_create_time,
                dss_update_time
            )
            VALUES (
                {% for column in columns %}
                SRC.{{ column }},
                {% endfor %}
                SRC.dss_create_time,
                SRC.dss_update_time
            )
    {% else %}
        CREATE OR REPLACE TABLE {{ target_table }} AS
        SELECT 
            {% for column in columns %}
            {{ column }},
            {% endfor %}
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time
        FROM {{ source_table }}
    {% endif %}
{% endmacro %}

