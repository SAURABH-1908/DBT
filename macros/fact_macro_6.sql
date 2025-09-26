{% macro merge_fct_forecasts_dynamic_1(
    target_table,
    source_table,
    join_keys,
    compare_columns,
    incremental_filter_column=none
) %}

    {%- set incremental_filter_column = incremental_filter_column or 'forecast_date' -%}

    {%- set target_columns = adapter.get_columns_in_relation(target_table) -%}
    {%- set column_names = target_columns | map(attribute='name') | list -%}

    {%- set source_columns = adapter.get_columns_in_relation(source_table) -%}
    {%- set source_column_names = source_columns | map(attribute='name') | list -%}

    {% set merge_sql %}
        MERGE INTO {{ target_table }} AS TGT
        USING (
            SELECT 
                {% for column in column_names %}
                {% if column in source_column_names %}
                    {{ column }}
                {% elif column == 'SYSTEM_CREATE_DATE' %}
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS SYSTEM_CREATE_DATE
                {% elif column == 'SYSTEM_UPDATE_DATE' %}
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS SYSTEM_UPDATE_DATE
                {% else %}
                    NULL AS {{ column }}
                {% endif %}
                {% if not loop.last %},{% endif %}
                {% endfor %}
            FROM {{ source_table }}
            {% if is_incremental() %}
            WHERE {{ incremental_filter_column }} > (
                SELECT COALESCE(MAX({{ incremental_filter_column }}), '1900-01-01') 
                FROM {{ target_table }}
            )
            {% endif %}
        ) AS SRC
        ON 
            {% for key in join_keys %}
            SRC.{{ key }} = TGT.{{ key }}{% if not loop.last %} AND {% endif %}
            {% endfor %}

        WHEN MATCHED AND (
            {% for column in compare_columns %}
            (SRC.{{ column }} != TGT.{{ column }} 
             OR (SRC.{{ column }} IS NULL AND TGT.{{ column }} IS NOT NULL)
             OR (SRC.{{ column }} IS NOT NULL AND TGT.{{ column }} IS NULL))
            {% if not loop.last %}OR{% endif %}
            {% endfor %}
        ) THEN UPDATE SET
            {% for column in column_names %}
            {% if column not in join_keys and column != 'SYSTEM_CREATE_DATE' %}
            {{ column }} = SRC.{{ column }}
            {% if not loop.last %},{% endif %}
            {% endif %}
            {% endfor %}

        WHEN NOT MATCHED THEN INSERT (
            {% for column in column_names %}
            {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        ) VALUES (
            {% for column in column_names %}
            SRC.{{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        )

    {% endset %}

    {{ log("Executing MERGE statement for: " ~ target_table) }}
    {{ log(merge_sql, info=true) }}
    {% do run_query(merge_sql) %}
    {{ log("MERGE statement completed successfully") }}

{% endmacro %}