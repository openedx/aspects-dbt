-- How many units, problems and videos each subsection has in its course's latest
-- publish.


select
    org,
    course_key,
    block_type,
    section_number,
    subsection_number,
    any(section_block_id) as section_block_id,
    any(subsection_block_id) as subsection_block_id,
    any(section_with_name) as section_with_name,
    any(subsection_with_name) as subsection_with_name,
    any(section_course_order) as section_course_order,
    any(subsection_course_order) as subsection_course_order,
    min(course_order) as course_order,
    max(graded) as graded,
    count() as item_count
from `xapi`.`dim_course_blocks`
where in_latest_publish and block_type in ('unit', 'problem', 'video')
group by org, course_key, block_type, section_number, subsection_number