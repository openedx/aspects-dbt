

with
    most_recent_course_blocks as (
        select
            location,
            display_name,
            toString(section)
            || ':'
            || toString(subsection)
            || ':'
            || toString(unit)
            || ' - '
            || display_name as display_name_with_location,
            JSONExtractInt(xblock_data_json, 'section') as section,
            JSONExtractInt(xblock_data_json, 'subsection') as subsection,
            JSONExtractInt(xblock_data_json, 'unit') as unit,
            JSONExtractBool(xblock_data_json, 'graded') as graded,
            `order` as course_order,
            course_key,
            dump_id,
            time_last_dumped,
            row_number() over (
                partition by location order by time_last_dumped desc
            ) as rn
        from `event_sink`.`course_blocks`
    )
select
    location,
    display_name as block_name,
    course_key,
    graded,
    course_order,
    display_name_with_location
from most_recent_course_blocks
where rn = 1