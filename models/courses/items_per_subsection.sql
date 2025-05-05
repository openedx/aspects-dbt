{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, block_type, section_number, subsection_number)",
    )
}}

select
    course_blocks.org as org,
    course_blocks.course_key as course_key,
    course_blocks.section_number as section_number,
    course_blocks.section_block_id as section_block_id,
    course_blocks.section_with_name as section_with_name,
    course_blocks.subsection_number as subsection_number,
    course_blocks.subsection_block_id as subsection_block_id,
    course_blocks.subsection_with_name as subsection_with_name,
    course_blocks.course_order as original_block_course_order,
    section.course_order as section_course_order,
    subsection.course_order as subsection_course_order,
    regexpExtract(course_blocks.block_id, '(?:@)(\S+)(?:@)') as block_type,
    count(course_blocks.block_id) as item_count
from {{ ref("dim_course_blocks") }} course_blocks
join
    {{ ref("dim_course_blocks") }} section
    on course_blocks.org = section.org
    and course_blocks.course_key = section.course_key
    and course_blocks.section_block_id = section.block_id
join
    {{ ref("dim_course_blocks") }} subsection
    on course_blocks.org = subsection.org
    and course_blocks.course_key = subsection.course_key
    and course_blocks.subsection_block_id = subsection.block_id
group by
    org,
    course_key,
    section_number,
    section_block_id,
    section_with_name,
    subsection_number,
    subsection_block_id,
    subsection_with_name,
    original_block_course_order,
    section_course_order,
    subsection_course_order,
    block_type
