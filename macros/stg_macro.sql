{% materialization stage_insert, default %}
 
  {%- set target_relation = this -%}
  {%- set tmp_relation = make_temp_relation(this) -%}
  {%- set truncate = config.get('truncate', False) -%}
 
  {% call statement('main') %}
 
    -- Step 1: Create table if it doesn't exist
    {% if not adapter.get_relation(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier) %}
 
        {{ log("Target table does not exist. Creating: " ~ target_relation, info=True) }}
 
        create table {{ target_relation }} as (
            {{ sql }}
        );
 
    {% else %}
 
        -- Step 2: Create a temp table with model logic
        {{ log("Creating temporary table: " ~ tmp_relation, info=True) }}
        create table {{ tmp_relation }} as (
            {{ sql }}
        );
 
        -- Step 3: Optionally truncate
        {% if truncate %}
            {{ log("Truncating target table: " ~ target_relation, info=True) }}
            truncate table {{ target_relation }};
        {% endif %}
 
        -- Step 4: Insert into target from temp
        {{ log("Inserting data into target table: " ~ target_relation, info=True) }}
        insert into {{ target_relation }}
        select * from {{ tmp_relation }};
 
        -- Step 5: Drop temp table
        {{ log("Dropping temp table: " ~ tmp_relation, info=True) }}
        drop table if exists {{ tmp_relation }};
 
    {% endif %}
 
  {% endcall %}
 
  {{ return({'relations': [target_relation]}) }}
 
{% endmaterialization %}
 