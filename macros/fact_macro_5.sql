{% macro merge_fact_table_enhanced_3(
    target_table,
    source_table,
    join_keys,
    columns,
    update_condition=none,
    incremental_filter_column=none
) %}

    {# Set default values if parameters are not provided #}
    {%- set update_condition = update_condition or "true" -%}
    {%- set incremental_filter_column = incremental_filter_column or 'dss_update_time' -%}

    {# Build the MERGE SQL statement #}
    {% set merge_sql %}
        MERGE INTO {{ target_table }} AS TGT
        USING (
            {# Select current/updated records from source table #}
            SELECT 
                {% for column in columns %}
                {{ column }}{% if not loop.last %},{% endif %}
                {% endfor %},
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_create_time,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dss_update_time
            FROM {{ source_table }}
            {% if is_incremental() %}
            {# Apply incremental filter to process only new/updated records #}
            WHERE {{ incremental_filter_column }} > (
                SELECT COALESCE(MAX({{ incremental_filter_column }}), '1900-01-01') 
                FROM {{ target_table }}
            )
            {% endif %}
        ) AS SRC
        ON 
            {# Build join condition using join keys #}
            {% for key in join_keys %}
            SRC.{{ key }} = TGT.{{ key }}{% if not loop.last %} AND {% endif %}
            {% endfor %}

        {# Update existing records when condition is met #}
        WHEN MATCHED AND (
            {{ update_condition }}
        ) THEN UPDATE SET
            {% set update_columns = [] %}
            {% for column in columns %}
                {% if column not in join_keys and column not in ['dss_create_time'] %}
                    {% do update_columns.append("TGT." ~ column ~ " = SRC." ~ column) %}
                {% endif %}
            {% endfor %}
            {% do update_columns.append("TGT.dss_update_time = SRC.dss_update_time") %}
            {{ update_columns | join(', ') }}

        {# Insert new records #}
        WHEN NOT MATCHED THEN INSERT (
            {% for column in columns %}
            {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %},
            dss_create_time,
            dss_update_time
        ) VALUES (
            {% for column in columns %}
            SRC.{{ column }}{% if not loop.last %},{% endif %}
            {% endfor %},
            SRC.dss_create_time,
            SRC.dss_update_time
        );
    {% endset %}

    {# Execute the MERGE statement #}
    {{ log("Executing MERGE statement for incremental update on table: " ~ target_table) }}
    {% do run_query(merge_sql) %}
    {{ log("MERGE statement completed successfully for table: " ~ target_table) }}

{% endmacro %}