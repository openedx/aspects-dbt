{% macro get_expected(table_name) %}

    {% if target.schema == 'reporting' %}

        select * from reporting.{table_name}
    {% else %}
        testestseted

    {% endif %}

{%- endmacro -%}
