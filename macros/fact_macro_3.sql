{% macro merge_fact_table_enhanced(
    target_table,
    source_table,
    join_keys,
    columns,
    update_condition=none,
    incremental_filter_column=none,
    handle_deletes=false,
    delete_column='dss_delete_time',
    track_changes=false
) %}

    {%- set update_condition = update_condition or "true" -%}
    {%- set incremental_filter_column = incremental_filter_column or 'dss_update_time' -%}
    
    {% if is_incremental() %}
        -- Build incremental filter condition
        {% set incremental_filter %}
            {% if incremental_filter_column %}
            WHERE {{ source_table }}.{{ incremental_filter_column }} > (
                SELECT COALESCE(MAX({{ incremental_filter_column }}), '1900-01-01') 
                FROM {{ target_table }}
            )
            {% endif %}
        {% endset %}

        MERGE INTO {{ target_table }} AS "TGT"
        USING (
            SELECT 
                {% for column in columns %}
                {{ column }},
                {% endfor %}
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
                CAST(NULL AS TIMESTAMP) AS {{ delete_column }}
            FROM {{ source_table }}
            {{ incremental_filter }}
            
            {% if handle_deletes %}
            -- Union with deleted records detection
            UNION ALL
            
            SELECT 
                {% for column in columns %}
                TGT.{{ column }},
                {% endfor %}
                TGT.dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS {{ delete_column }}
            FROM {{ target_table }} TGT
            LEFT JOIN {{ source_table }} SRC
                ON {% for key in join_keys %}SRC.{{ key }} = TGT.{{ key }}{% if not loop.last %} AND {% endif %}{% endfor %}
            WHERE SRC.{{ join_keys[0] }} IS NULL
                AND (TGT.{{ delete_column }} IS NULL OR TGT.{{ delete_column }} > CURRENT_TIMESTAMP - INTERVAL '1 DAY')
            {% endif %}
        ) AS "SRC"
        ON 
            {% for key in join_keys %}
            SRC.{{ key }} = TGT.{{ key }} {% if not loop.last %}AND{% endif %}
            {% endfor %}
        
        WHEN MATCHED AND ({{ update_condition }}) AND SRC.{{ delete_column }} IS NULL THEN
            UPDATE SET
                {% for column in columns if column not in join_keys %}
                TGT.{{ column }} = SRC.{{ column }}{% if not loop.last %},{% endif %}
                {% endfor %}
                TGT.dss_update_time = SRC.dss_update_time,
                TGT.{{ delete_column }} = SRC.{{ delete_column }}
        
        WHEN MATCHED AND SRC.{{ delete_column }} IS NOT NULL THEN
            UPDATE SET
                TGT.dss_update_time = SRC.dss_update_time,
                TGT.{{ delete_column }} = SRC.{{ delete_column }}
        
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
            )
    
    {% else %}
        -- Initial full load
        CREATE OR REPLACE TABLE {{ target_table }} AS
        SELECT 
            {% for column in columns %}
            {{ column }},
            {% endfor %}
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time,
            CAST(NULL AS TIMESTAMP) AS {{ delete_column }}
        FROM {{ source_table }}
        
        {% if track_changes %}
        -- Add change tracking metadata columns
        ALTER TABLE {{ target_table }} ADD COLUMN dss_change_type STRING DEFAULT 'INSERT';
        ALTER TABLE {{ target_table }} ADD COLUMN dss_change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        {% endif %}
    {% endif %}
    
    {% if track_changes and is_incremental() %}
    -- Create change tracking view (optional)
    CREATE OR REPLACE VIEW {{ target_table }}_changes AS
    SELECT 
        *,
        CASE 
            WHEN {{ delete_column }} IS NOT NULL THEN 'DELETE'
            WHEN dss_create_time = dss_update_time THEN 'INSERT' 
            ELSE 'UPDATE'
        END AS dss_change_type
    FROM {{ target_table }}
    WHERE dss_update_time > (
        SELECT COALESCE(MAX(dss_update_time), '1900-01-01') 
        FROM {{ target_table }} 
        WHERE dss_update_time < CURRENT_TIMESTAMP - INTERVAL '1 minute'
    )
    {% endif %}
{% endmacro %}