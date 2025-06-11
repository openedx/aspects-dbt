with
    dim_course_blocks as (
        select
            courses.org as org,
            courses.course_key as course_key,
            courses.course_name as course_name,
            courses.course_run as course_run,
            blocks.location as block_id,
            blocks.block_name as block_name,
            {{ section_from_display("blocks.display_name_with_location") }}
            as section_number,
            {{ subsection_from_display("blocks.display_name_with_location") }}
            as subsection_number,
            splitByString(' - ', blocks.display_name_with_location)[
                1
            ] as hierarchy_location,
            blocks.display_name_with_location as display_name_with_location,
            course_order,
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
        from {{ ref("dim_course_block_names") }} blocks
        join
            {{ ref("dim_course_names") }} courses
            on blocks.course_key = courses.course_key
    )
select
    blocks.org as org,
    blocks.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    blocks.block_id as block_id,
    section_blocks.block_id as section_block_id,
    subsection_blocks.block_id as subsection_block_id,
    blocks.block_name as block_name,
    blocks.section_number as section_number,
    blocks.subsection_number as subsection_number,
    blocks.hierarchy_location as hierarchy_location,
    blocks.display_name_with_location as display_name_with_location,
    blocks.course_order as course_order,
    blocks.graded as graded,
    blocks.block_type as block_type,
    section_blocks.display_name_with_location as section_with_name,
    subsection_blocks.display_name_with_location as subsection_with_name
from dim_course_blocks as blocks
left join
    dim_course_blocks as section_blocks
    on (
        blocks.section_number = section_blocks.hierarchy_location
        and blocks.org = section_blocks.org
        and blocks.course_key = section_blocks.course_key
        and section_blocks.block_id like '%@chapter+block@%'
    )
left join
    dim_course_blocks as subsection_blocks
    on (
        blocks.subsection_number = subsection_blocks.hierarchy_location
        and blocks.org = subsection_blocks.org
        and blocks.course_key = subsection_blocks.course_key
        and subsection_blocks.block_id like '%@sequential+block@%'
    )
