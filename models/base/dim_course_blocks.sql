select
    courses.org as org,
    courses.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    blocks.location as block_id,
    blocks.block_name as block_name,
    {{ section_from_display("blocks.display_name_with_location") }} as section_number,
    {{ subsection_from_display("blocks.display_name_with_location") }}
    as subsection_number,
    splitByString(' - ', blocks.display_name_with_location)[1] as hierarchy_location,
    blocks.display_name_with_location as display_name_with_location,
    graded,
    case
        when block_id like '%@chapter+block@%'
        then 'section'
        when block_id like '%@sequential+block@%'
        then 'subsection'
        when block_id like '%@vertical+block@%'
        then 'unit'
        else regexpExtract(block_id, '@([^+]+)\+block@', 1)
    end as block_type
from {{ source("event_sink", "course_block_names") }} blocks
join
    {{ source("event_sink", "course_names") }} courses
    on blocks.course_key = courses.course_key
    settings join_algorithm = 'direct'
