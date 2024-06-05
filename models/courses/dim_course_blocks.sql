select
    courses.org as org,
    courses.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    blocks.location as block_id,
    blocks.block_name as block_name,
    blocks.section_id as section_id,
    blocks.subsection_id as subsection_id,
    blocks.unit_id as unit_id,
    sections.location as section_block_id,
    sections.section_name_with_location as section_name_with_location,
    subsections.location as subsection_block_id,
    subsections.subsection_name_with_location as subsection_name_with_location,
    blocks.display_name_with_location as display_name_with_location,
    course_order as course_order,
    graded as graded,
    case
        when block_id like '%@chapter+block@%'
        then 'section'
        when block_id like '%@sequential+block@%'
        then 'subsection'
        when block_id like '%@vertical+block@%'
        then 'unit'
        else regexpExtract(block_id, '@([^+]+)\+block@', 1)
    end as block_type
from {{ ref("course_block_names") }} blocks
join
    {{ ref("course_names") }} courses on blocks.course_key = courses.course_key
join
    {{ ref("section_block_names") }} sections on blocks.course_key = sections.course_key
        and blocks.section_id = sections.section_id
join
    {{ ref("subsection_block_names") }} subsections on blocks.course_key = subsections.course_key
    and blocks.section_id = subsections.section_id
    and blocks.subsection_id = subsections.subsection_id
