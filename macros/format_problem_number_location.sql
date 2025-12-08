{% macro format_problem_location(object_id_var, display_name_with_location_var) %}
    substring(
        regexpExtract({{ object_id_var }}, '(@problem\+block@[\w\d][^_\/]*)(_\d)?', 2),
        2
    ) as _problem_id_number,
    ifNull(nullIf(_problem_id_number, ''), '1') as _problem_id_or_1,
    splitByString(' - ', {{ display_name_with_location_var }}) as _problem_with_name,
    arrayStringConcat(
        arrayMap(
            x -> (leftPad(x, 2, char(917768))),
            splitByString(':', _problem_with_name[1])
        ),
        ':'
    ) as _problem_number,
    concat(_problem_number, '_', _problem_id_or_1) as problem_location,
    concat(problem_location, ' - ', _problem_with_name[2]) as problem_name_location
{% endmacro %}

{% macro format_object_location(display_name_with_location_var) %}
    arrayStringConcat(
        arrayMap(
            x -> (leftPad(x, 2, char(917768))),
            splitByString(
                ':', splitByString(' - ', {{ display_name_with_location_var }})[1]
            )
        ),
        ':'
    ) as object_location,
    concat(
        object_location,
        ' - ',
        splitByString(' - ', {{ display_name_with_location_var }})[2]
    ) as object_name_location
{% endmacro %}
