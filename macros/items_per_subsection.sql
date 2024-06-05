{% macro items_per_subsection(block_pattern) %}
    select
        org,
        course_key,
        section_block_id,
        section_name_with_location,
        subsection_block_id,
        subsection_name_with_location,
        course_order,
        graded,
        count(*) as item_count
    from {{ ref("dim_course_blocks") }}
    where block_id like '{{ block_pattern }}'
    group by
        org, course_key, section_block_id, section_name_with_location, subsection_block_id, subsection_name_with_location, course_order, graded
{% endmacro %}
