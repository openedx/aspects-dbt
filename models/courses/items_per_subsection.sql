{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(original_block_id, org, course_key, section_number, subsection_number)",
    )
}}

select
    ips.block_id as original_block_id,
    ips.org as org,
    ips.course_key as course_key,
    ips.section_number as section_number,
    section_blocks.block_id as section_block_id,
    section_blocks.display_name_with_location as section_with_name,
    ips.subsection_number as subsection_number,
    subsection_blocks.block_id as subsection_block_id,
    subsection_blocks.display_name_with_location as subsection_with_name,
    ips.course_order as course_order
from reporting.dim_course_blocks ips
left join
    reporting.dim_course_blocks section_blocks
    on (
        ips.section_number = section_blocks.hierarchy_location
        and ips.org = section_blocks.org
        and ips.course_key = section_blocks.course_key
        and section_blocks.block_id like '%@chapter+block@%'
    )
left join
    reporting.dim_course_blocks subsection_blocks
    on (
        ips.subsection_number = subsection_blocks.hierarchy_location
        and ips.org = subsection_blocks.org
        and ips.course_key = subsection_blocks.course_key
        and subsection_blocks.block_id like '%@sequential+block@%'
    )
