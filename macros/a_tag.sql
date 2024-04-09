{% macro a_tag(object_id, object_name) %}
  concat('<a href="', {{ object_id }}, '" target="_blank">', {{ object_name }}, '</a>')
{% endmacro %}
