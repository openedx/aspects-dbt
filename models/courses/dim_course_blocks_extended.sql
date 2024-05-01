select
    blocks.org as org,
    blocks.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    blocks.block_id as block_id,
    blocks.block_name as block_name,
    blocks.section_number as section_number,
    blocks.subsection_number as subsection_number,
    blocks.hierarchy_location as hierarchy_location,
    blocks.display_name_with_location as display_name_with_location,
    blocks.graded as graded,
    blocks.course_order as course_order,
    blocks.block_type as block_type,
    section_blocks.display_name_with_location as section_with_name,
    subsection_blocks.display_name_with_location as subsection_with_name
from {{ ref("dim_course_blocks") }} blocks
left join
    {{ ref("dim_course_blocks") }} section_blocks
    on (
        blocks.section_number = section_blocks.hierarchy_location
        and blocks.org = section_blocks.org
        and blocks.course_key = section_blocks.course_key
        and section_blocks.block_id like '%@chapter+block@%'
    )
left join
    {{ ref("dim_course_blocks") }} subsection_blocks
    on (
        blocks.subsection_number = subsection_blocks.hierarchy_location
        and blocks.org = subsection_blocks.org
        and blocks.course_key = subsection_blocks.course_key
        and subsection_blocks.block_id like '%@sequential+block@%'
    )
