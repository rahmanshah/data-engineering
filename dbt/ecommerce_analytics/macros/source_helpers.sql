{% macro source_query(source_name, table_name) %}
  {% set source_node = source(source_name, table_name) %}
  {% set external_location = source_node.meta.get('external_location') %}

  {% if external_location %}
    {{ return(external_location) }}
  {% else %}
    {{ return('select * from ' ~ source_node) }}
  {% endif %}
{% endmacro %}