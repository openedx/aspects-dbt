{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(original_block_id, org, course_key, section_number, subsection_number)",
    )
}}

select
    block_id as original_block_id,
    org,
    course_key,
    section_number,
    section_block_id,
    section_with_name,
    subsection_number,
    subsection_block_id,
    subsection_with_name,
    course_order
from {{ ref('dim_course_blocks') }}
