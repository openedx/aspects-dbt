{% macro format_problem_number_location(
    object_id_var, display_name_with_location_var
) %}
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
    concat(_problem_number, '_', _problem_id_or_1) as problem_number,
    concat(problem_number, ' - ', _problem_with_name[2]) as problem_name_location
{% endmacro %}
