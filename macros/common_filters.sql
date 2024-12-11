{% macro common_filters() -%}
    and (
        (
            {org_filter:String} <> '[]'
            and org in cast({org_filter:String}, 'Array(String)')
        )
        or {org_filter:String} = '[]'
    )
    and (
        (
            {course_key_filter:String} <> '[]'
            and course_key in cast({course_key_filter:String}, 'Array(String)')
        )
        or {course_key_filter:String} = '[]'
    )
{%- endmacro %}