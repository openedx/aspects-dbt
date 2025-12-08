{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, block_id, actor_id)",
        order_by="(org, course_key, block_id, actor_id)",
    )
}}

select
    navigation.org as org,
    navigation.course_key as course_key,
    navigation.block_id as block_id,
    pages.subsection_course_order as course_order,
    navigation.actor_id as actor_id,
    pages.item_count as page_count,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    max(date(navigation.emission_time)) as visited_on,
    pages.subsection_block_id as subsection_block_id,
    pages.section_block_id as section_block_id
from {{ ref("navigation_events") }} navigation
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        navigation.course_key = blocks.course_key
        and navigation.block_id = blocks.block_id
    )
join
    {{ ref("dim_items_per_subsection") }} pages
    on (
        pages.org = navigation.org
        and pages.course_key = navigation.course_key
        and pages.section_number = blocks.section_number
        and pages.subsection_number = blocks.subsection_number
    )
where pages.block_type = 'vertical+block'
group by
    org,
    course_key,
    block_id,
    course_order,
    actor_id,
    page_count,
    section_with_name,
    subsection_with_name,
    subsection_block_id,
    section_block_id
