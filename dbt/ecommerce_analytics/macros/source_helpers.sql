{% macro source_query(source_name, table_name) %}
  {% set source_node = source(source_name, table_name) %}
  {% set external_location = adapter.get_relation(database=none, schema=none, identifier=table_name).meta.get('external_location', none) or source(source_name, table_name).meta.get('external_location') %}

  {% if external_location %}
    {{ external_location }}
  {% else %}
    select * from {{ source_node }}
  {% endif %}
{% endmacro %}