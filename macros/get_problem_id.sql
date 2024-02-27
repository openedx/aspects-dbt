-- extract the problem ID from the object ID
-- either the problem id is at the end of the object ID
-- or is followed by '/answer' or '/hint'
{% macro get_problem_id(object_id) %}
    regexpExtract(
        object_id, 'xblock/([\w\d-\+:@]*@problem\+block@[\w\d][^_]*)(_\d_\d)?', 1
    )
{% endmacro %}
