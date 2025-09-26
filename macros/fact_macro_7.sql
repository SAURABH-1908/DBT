{% macro fact_merge(target_relation, source_relation, unique_key, system_columns=['SYSTEM_CREATE_DATE', 'SYSTEM_UPDATE_DATE']) %}

{% set source_columns = adapter.get_columns_in_relation(source_relation) %}

{% set merge_sql %}
MERGE INTO {{ target_relation }} AS TGT
USING (
    SELECT
        {% for column in source_columns %}
            SRC."{{ column.name }}"{% if not loop.last %},{% endif %}
        {% endfor %},
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS "{{ system_columns[0] }}",
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS "{{ system_columns[1] }}"
    FROM {{ source_relation }} AS SRC
) AS SRC
ON {% for key in unique_key %}SRC."{{ key | upper }}" = TGT."{{ key | upper }}"{% if not loop.last %} AND {% endif %}{% endfor %}

WHEN MATCHED AND (
    {% for column in source_columns %}
        {% if column.name.upper() not in system_columns and column.name.upper() not in unique_key | map('upper') | list %}
            NVL(CAST(SRC."{{ column.name }}" AS STRING), '**NULL**') <> NVL(CAST(TGT."{{ column.name }}" AS STRING), '**NULL**'){% if not loop.last %} OR
            {% endif %}
        {% endif %}
    {% endfor %}
) THEN UPDATE SET
    {% for column in source_columns %}
        {% if column.name.upper() not in system_columns and column.name.upper() not in unique_key | map('upper') | list %}
            TGT."{{ column.name }}" = SRC."{{ column.name }}"{% if not loop.last %},
            {% endif %}
        {% endif %}
    {% endfor %},
    TGT."{{ system_columns[1] }}" = SRC."{{ system_columns[1] }}"

WHEN NOT MATCHED THEN
INSERT (
    {% for column in source_columns %}
        "{{ column.name }}",
    {% endfor %}
    "{{ system_columns[0] }}",
    "{{ system_columns[1] }}"
)
VALUES (
    {% for column in source_columns %}
        SRC."{{ column.name }}",
    {% endfor %}
    SRC."{{ system_columns[0] }}",
    SRC."{{ system_columns[1] }}"
);
{% endset %}

{{ log("Running MERGE SQL:\n" ~ merge_sql, info=True) }}

{% do run_query(merge_sql) %}

{% endmacro %}
