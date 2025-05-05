with
    final_results as (
        select
            navigation.org as org,
            navigation.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            items.subsection_course_order as course_order,
            navigation.actor_id as actor_id,
            items.original_block_id as original_block_id,
            navigation.block_id as block_id,
            items.section_block_id as section_block_id,
            items.section_number as section_number,
            items.section_with_name as section_with_name,
            items.subsection_block_id as subsection_block_id,
            items.subsection_number as subsection_number,
            items.subsection_with_name as subsection_with_name,
            date(navigation.emission_time) as visited_on
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
    course_name,
    course_run,
    actor_id,
    block_id,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name,
    original_block_id
from final_results
