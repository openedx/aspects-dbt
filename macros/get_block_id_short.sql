-- A presentation field only. Full usage keys belong in joins and storage keys.
{% macro get_block_id_short(block_id) %}
    extract({{ block_id }}, '[+]block@([^+/?#]+)$')
{% endmacro %}
