--soft delete is not handled in this macro if we delete anything then it wont be removed from here


{% macro type2_dimension_11(
    source_model,
    target_model,
    business_key,
    type2_columns,
    non_type2_columns,
    current_flag_column='current_flag',
    version_column='version',
    start_date_column='valid_from',
    end_date_column='valid_to',
    create_date_column='created_at',
    update_date_column='updated_at'
) %}

{% set business_keys = business_key.split(',') %}
{% set type2_cols = type2_columns.split(',') %}
{% set non_type2_cols = non_type2_columns.split(',') %}

{{ log("Running custom SCD Type 2 merge for " ~ target_model, info=true) }}

{% call statement('type2_scd', fetch_result=false, auto_begin=true) %}
MERGE INTO {{ target_model }} AS TGT USING (
  -- New Records (Insert)
  SELECT 
    {% for col in business_keys %}SRC.{{ col }},{% endfor %}
    {% for col in type2_cols %}SRC.{{ col }},{% endfor %}
    {% for col in non_type2_cols %}SRC.{{ col }},{% endfor %}
    1 AS {{ version_column }},
    'Y' AS {{ current_flag_column }},
    CURRENT_TIMESTAMP() AS {{ start_date_column }},
    CAST('2999-12-31' AS TIMESTAMP) AS {{ end_date_column }},
    CURRENT_TIMESTAMP() AS {{ create_date_column }},
    CURRENT_TIMESTAMP() AS {{ update_date_column }}
  FROM {{ source_model }} AS SRC
  LEFT JOIN {{ target_model }} AS DIM 
    ON 1=1
    {% for key in business_keys %}
    AND SRC.{{ key }} = DIM.{{ key }}
    {% endfor %}
  WHERE 
    DIM.{{ current_flag_column }} IS NULL  -- Only new records

  UNION ALL
  -- Type-2 Changes (New Version)
  SELECT 
    {% for col in business_keys %}SRC.{{ col }},{% endfor %}
    {% for col in type2_cols %}SRC.{{ col }},{% endfor %}
    {% for col in non_type2_cols %}SRC.{{ col }},{% endfor %}
    DIM.{{ version_column }} + 1,
    'Y' AS {{ current_flag_column }},
    CURRENT_TIMESTAMP() AS {{ start_date_column }},
    CAST('2999-12-31' AS TIMESTAMP) AS {{ end_date_column }},
    CURRENT_TIMESTAMP() AS {{ create_date_column }},
    CURRENT_TIMESTAMP() AS {{ update_date_column }}
  FROM {{ source_model }} AS SRC
  INNER JOIN {{ target_model }} AS DIM 
    ON 1=1
    {% for key in business_keys %}
    AND SRC.{{ key }} = DIM.{{ key }}
    {% endfor %}
  WHERE DIM.{{ current_flag_column }} = 'Y'
    AND (
      {% for col in type2_cols %}
      COALESCE(SRC.{{ col }}::VARCHAR, '') <> COALESCE(DIM.{{ col }}::VARCHAR, ''){% if not loop.last %} OR {% endif %}
      {% endfor %}
    )

  UNION ALL
  -- Expire Old Versions
  SELECT 
    {% for col in business_keys %}DIM.{{ col }},{% endfor %}
    {% for col in type2_cols %}DIM.{{ col }},{% endfor %}
    {% for col in non_type2_cols %}DIM.{{ col }},{% endfor %}
    DIM.{{ version_column }},
    'N' AS {{ current_flag_column }},
    DIM.{{ start_date_column }},
    DATEADD(MILLISECOND, -1, CURRENT_TIMESTAMP()),
    DIM.{{ create_date_column }},
    CURRENT_TIMESTAMP()
  FROM {{ source_model }} AS SRC
  INNER JOIN {{ target_model }} AS DIM 
    ON 1=1
    {% for key in business_keys %}
    AND SRC.{{ key }} = DIM.{{ key }}
    {% endfor %}
  WHERE DIM.{{ current_flag_column }} = 'Y'
    AND (
      {% for col in type2_cols %}
      COALESCE(SRC.{{ col }}::VARCHAR, '') <> COALESCE(DIM.{{ col }}::VARCHAR, ''){% if not loop.last %} OR {% endif %}
      {% endfor %}
    )

  UNION ALL
  -- Non-Type2 Updates
  SELECT 
    {% for col in business_keys %}SRC.{{ col }},{% endfor %}
    {% for col in type2_cols %}DIM.{{ col }},{% endfor %}  -- Keep original
    {% for col in non_type2_cols %}SRC.{{ col }},{% endfor %}  -- New values
    DIM.{{ version_column }},
    DIM.{{ current_flag_column }},
    DIM.{{ start_date_column }},
    DIM.{{ end_date_column }},
    DIM.{{ create_date_column }},
    CURRENT_TIMESTAMP()
  FROM {{ source_model }} AS SRC
  INNER JOIN {{ target_model }} AS DIM 
    ON 1=1
    {% for key in business_keys %}
    AND SRC.{{ key }} = DIM.{{ key }}
    {% endfor %}
  WHERE DIM.{{ current_flag_column }} = 'Y'
    AND NOT (
      {% for col in type2_cols %}
      COALESCE(SRC.{{ col }}::VARCHAR, '') <> COALESCE(DIM.{{ col }}::VARCHAR, ''){% if not loop.last %} OR {% endif %}
      {% endfor %}
    )
    AND (
      {% for col in non_type2_cols %}
      COALESCE(SRC.{{ col }}::VARCHAR, '') <> COALESCE(DIM.{{ col }}::VARCHAR, ''){% if not loop.last %} OR {% endif %}
      {% endfor %}
    )
) AS SRC ON 
  {% for key in business_keys %}
  TGT.{{ key }} = SRC.{{ key }} AND
  {% endfor %}
  TGT.{{ version_column }} = SRC.{{ version_column }}
WHEN MATCHED THEN UPDATE SET
  {% for col in non_type2_cols %}
  TGT.{{ col }} = SRC.{{ col }},
  {% endfor %}
  TGT.{{ current_flag_column }} = SRC.{{ current_flag_column }},
  TGT.{{ start_date_column }} = SRC.{{ start_date_column }},
  TGT.{{ end_date_column }} = SRC.{{ end_date_column }},
  TGT.{{ update_date_column }} = SRC.{{ update_date_column }}
WHEN NOT MATCHED THEN INSERT (
    {% for col in business_keys %}{{ col }},{% endfor %}
    {% for col in type2_cols %}{{ col }},{% endfor %}
    {% for col in non_type2_cols %}{{ col }},{% endfor %}
    {{ version_column }},
    {{ current_flag_column }},
    {{ start_date_column }},
    {{ end_date_column }},
    {{ create_date_column }},
    {{ update_date_column }}
  ) VALUES (
    {% for col in business_keys %}SRC.{{ col }},{% endfor %}
    {% for col in type2_cols %}SRC.{{ col }},{% endfor %}
    {% for col in non_type2_cols %}SRC.{{ col }},{% endfor %}
    SRC.{{ version_column }},
    SRC.{{ current_flag_column }},
    SRC.{{ start_date_column }},
    SRC.{{ end_date_column }},
    SRC.{{ create_date_column }},
    SRC.{{ update_date_column }}
  );
{% endcall %}

{% endmacro %}