with
    pages_per_subsection as (
        select * from ({{ items_per_subsection("%@vertical+block@%") }})
    )
select
    navigation.org as org,
    navigation.course_key as course_key,
    navigation.block_id as block_id,
    pages.subsection_course_order as course_order,
    navigation.actor_id as actor_id,
    pages.item_count as page_count,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    date(navigation.emission_time) as visited_on
from {{ ref("navigation_events") }} navigation
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        navigation.course_key = blocks.course_key
        and navigation.block_id = blocks.block_id
    )
join
    pages_per_subsection pages
    on (
        pages.org = navigation.org
        and pages.course_key = navigation.course_key
        and pages.section_number = blocks.section_number
        and pages.subsection_number = blocks.subsection_number
    )
