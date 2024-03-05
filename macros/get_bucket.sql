{% macro get_bucket(field) -%}
    case
        when {{ field }} >= 0.9
        then '90-100%'
        when {{ field }} >= 0.8 and {{ field }} < 0.9
        then '80-89%'
        when {{ field }} >= 0.7 and {{ field }} < 0.8
        then '70-79%'
        when {{ field }} >= 0.6 and {{ field }} < 0.7
        then '60-69%'
        when {{ field }} >= 0.5 and {{ field }} < 0.6
        then '50-59%'
        when {{ field }} >= 0.4 and {{ field }} < 0.5
        then '40-49%'
        when {{ field }} >= 0.3 and {{ field }} < 0.4
        then '30-39%'
        when {{ field }} >= 0.2 and {{ field }} < 0.3
        then '20-29%'
        when {{ field }} >= 0.1 and {{ field }} < 0.2
        then '10-19%'
        else '0-9%'
    end
{%- endmacro %}
