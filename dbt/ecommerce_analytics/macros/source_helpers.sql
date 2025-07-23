{% macro source_query(source_node) %}
  {% set source_relation = source(source_node.source_name, source_node.name) %}
  {% set external_location = source_node.meta.get('external_location') %}

  {% if external_location %}
    {{ external_location }}
  {% else %}
    select * from {{ source_relation }}
  {% endif %}

{% endmacro %}