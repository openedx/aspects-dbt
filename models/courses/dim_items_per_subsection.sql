{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, block_type, section_number, subsection_number)",
        order_by="(org, course_key, block_type, section_number, subsection_number)",
    )
}}

select
    ips.org as org,
    ips.course_key as course_key,
    ips.section_number as section_number,
    section_blocks.display_name_with_location as section_with_name,
    ips.subsection_number as subsection_number,
    subsection_blocks.display_name_with_location as subsection_with_name,
    count(*) as item_count,
    subsection_blocks.block_id as subsection_block_id,
    section_blocks.block_id as section_block_id,
    subsection_blocks.course_order as subsection_course_order,
    case
        when ips.block_id like '%vertical+block%'
        then 'vertical+block'
        when ips.block_id like '%problem+block%'
        then 'problem+block'
        when ips.block_id like '%video+block%'
        then 'video+block'
        else 'none'
    end as block_type
from {{ ref("dim_course_blocks") }} ips
left join
    {{ ref("dim_course_blocks") }} section_blocks
    on (
        ips.section_number = section_blocks.hierarchy_location
        and ips.org = section_blocks.org
        and ips.course_key = section_blocks.course_key
        and section_blocks.block_id like '%@chapter+block@%'
    )
left join
    {{ ref("dim_course_blocks") }} subsection_blocks
    on (
        ips.subsection_number = subsection_blocks.hierarchy_location
        and ips.org = subsection_blocks.org
        and ips.course_key = subsection_blocks.course_key
        and subsection_blocks.block_id like '%@sequential+block@%'
    )
where block_type <> 'none'
group by
    org,
    course_key,
    section_number,
    section_with_name,
    subsection_number,
    subsection_with_name,
    subsection_block_id,
    section_block_id,
    subsection_course_order,
    block_type
