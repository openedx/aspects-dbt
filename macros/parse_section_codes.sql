-- truncate a block's location code (e.g. '3:1:4') to the subsection level
{% macro subsection_from_display(display_name_with_location) %}
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', {{ display_name_with_location }})[1], 2
            ),
            ':'
        ),
        ':0'
    )
{% endmacro %}

-- truncate a block's location code (e.g. '3:1:4') to the section level
{% macro section_from_display(display_name_with_location) %}
    concat(
        splitByString(
            ':', splitByString(' - ', {{ display_name_with_location }})[1], 1
        )[1],
        ':0:0'
    )
{% endmacro %}
