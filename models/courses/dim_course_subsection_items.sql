-- How many units, problems and videos each subsection has in its course's latest publish.
{{
    config(
        materialized="materialized_view",
        engine=get_engine("MergeTree()"),
        order_by="(org, course_key, block_type, section_number, subsection_number)",
        refreshable={
            "interval": env_var("ASPECTS_COURSE_STRUCTURE_REFRESH", "EVERY 5 MINUTE"),
            "depends_on": [this.schema ~ ".dim_course_blocks_mv"],
        },
        post_hook=[
            "system refresh view {{ this.schema }}.{{ this.identifier }}_mv",
            "system wait view {{ this.schema }}.{{ this.identifier }}_mv",
        ],
    )
}}

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
from {{ ref("dim_course_blocks") }}
where in_latest_publish and block_type in ('unit', 'problem', 'video')
group by org, course_key, block_type, section_number, subsection_number
