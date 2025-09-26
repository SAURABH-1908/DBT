{% macro generic_link_insert_1(
    target_relation,
    source_relation,
    source_columns,
    link_hashkey,
    ldts_alias,
    disable_hwm=false,
    timestamp_format='YYYY-MM-DD HH:MI:SS',
    beginning_of_all_times="'1900-01-01 00:00:00'"
) %}
    INSERT INTO {{ target_relation }}

    WITH incoming AS (
        SELECT DISTINCT
            {%- for col in source_columns -%}
                {{ col }} AS "{{ col }}"
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        FROM {{ source_relation }}
        {%- if not disable_hwm %}
            WHERE "{{ ldts_alias }}" > (
                SELECT
                    COALESCE(MAX("{{ ldts_alias }}"),
                    {{ beginning_of_all_times }})
                FROM {{ target_relation }}
            )
        {%- endif %}
    ),
    new_records AS (
        SELECT
            {%- for col in source_columns -%}
                "SRC"."{{ col }}"
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        FROM incoming "SRC"
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ target_relation }} "TGT"
            WHERE "SRC"."{{ link_hashkey }}" = "TGT"."{{ link_hashkey }}"
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "{{ link_hashkey }}"
            ORDER BY "{{ ldts_alias }}"
        ) = 1
    )
    SELECT
        {%- for col in source_columns -%}
            "SRC"."{{ col }}"
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    FROM new_records "SRC"
{% endmacro %}
