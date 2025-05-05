select
    org,
    course_key,
    course_order,
    actor_id,
    'section' as section_content_level,
    'subsection' as subsection_content_level,
    count(original_block_id) as item_count,
    count(distinct block_id) as pages_visited,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
from {{ ref("fact_navigation") }}
group by
    org,
    course_key,
    course_order,
    actor_id,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
