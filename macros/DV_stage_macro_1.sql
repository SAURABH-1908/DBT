{% macro data_vault_stage(columns, sources, config) %}
  WITH regular_columns AS (
      {% for source_info in sources %}
          SELECT
              {% set ns = namespace(first_col=true) %}
              {% for col in source_info.columns %}
                  {% if not col.is_ldts_column and not col.is_rsrc_column %}
                      {% if not ns.first_col %},{% endif %}
                      {{ source_info.alias }}."{{ col.name }}" AS "{{ col.name }}"
                      {% set ns.first_col = false %}
                  {% endif %}
              {% endfor %}
              {% for col in source_info.columns %}
                  {% if col.is_ldts_column %}
                      {% if not ns.first_col %},{% endif %}
                      SYSDATE() AS "{{ col.name }}"
                      {% set ns.first_col = false %}
                  {% endif %}
              {% endfor %}
              {% for col in source_info.columns %}
                  {% if col.is_rsrc_column %}
                      {% if not ns.first_col %},{% endif %}
                      '{{ source_info.source_name }}.{{ source_info.table_name }}' AS "{{ col.name }}"
                      {% set ns.first_col = false %}
                  {% endif %}
              {% endfor %}
          FROM {{ source(source_info.source_name, source_info.table_name) }} AS {{ source_info.alias }}
          {% if not loop.last %}
          UNION ALL
          {% endif %}
      {% endfor %}
  ),
  all_columns AS (
      SELECT * FROM regular_columns
      {%- if config.generate_ghost_records %}
      
      UNION ALL
      SELECT
          {% for col in columns %}
              {% if not loop.first %},{% endif %}
              {% if col.data_type in ['NUMBER', 'INT', 'INTEGER', 'FLOAT', 'DECIMAL'] %}
                  0
              {% elif col.data_type in ['VARCHAR', 'STRING', 'TEXT', 'CHAR'] %}
                  {% if col.is_rsrc_column %}
                      '(unknown)'
                  {% else %}
                      ''
                  {% endif %}
              {% elif col.data_type in ['TIMESTAMP', 'DATE', 'DATETIME'] %}
                  TO_TIMESTAMP('0001-01-01T00:00:01', 'YYYY-MM-DDTHH24:MI:SS')
              {% else %}
                  NULL
              {% endif %} AS "{{ col.name }}"
          {% endfor %}
      
      UNION ALL
      SELECT
          {% for col in columns %}
              {% if not loop.first %},{% endif %}
              {% if col.data_type in ['NUMBER', 'INT', 'INTEGER', 'FLOAT', 'DECIMAL'] %}
                  -1
              {% elif col.data_type in ['VARCHAR', 'STRING', 'TEXT', 'CHAR'] %}
                  '(error)'
              {% elif col.data_type in ['TIMESTAMP', 'DATE', 'DATETIME'] %}
                  TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')
              {% else %}
                  NULL
              {% endif %} AS "{{ col.name }}"
          {% endfor %}
      {%- endif %}
  )
  
  SELECT * FROM all_columns
{% endmacro %}
