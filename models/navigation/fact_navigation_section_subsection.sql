with
    get_navigation_data as (
        select
            navigation.org as org,
            navigation.course_key as course_key,
            blocks.course_order as course_order,
            navigation.actor_id as actor_id,
            items.original_block_id as original_block_id,
            blocks.block_id as page_id,
            items.section_block_id as section_block_id,
            items.subsection_block_id as subsection_block_id,
            items.section_with_name as section_with_name,
            items.subsection_with_name as subsection_with_name
        from {{ ref("navigation_events") }} navigation
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                navigation.course_key = blocks.course_key
                and navigation.block_id = blocks.block_id
            )
        join
            {{ ref("items_per_subsection") }} items
            on (
                items.org = navigation.org
                and items.course_key = navigation.course_key
                and items.section_number = blocks.section_number
                and items.subsection_number = blocks.subsection_number
            )
        where items.original_block_id like '%@vertical+block@%'
    )
select
    org,
    course_key,
    course_order,
    actor_id,
    'section' as section_content_level,
    'subsection' as subsection_content_level,
    count(original_block_id) as item_count,
    count(distinct page_id) as pages_visited,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
from get_navigation_data
group by
    org,
    course_key,
    course_order,
    actor_id,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
