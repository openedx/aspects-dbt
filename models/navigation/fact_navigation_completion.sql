select
    visits.org as org,
    visits.course_key as course_key,
    visits.block_id as block_id,
    pages.subsection_course_order as course_order,
    visits.actor_id as actor_id,
    pages.item_count as page_count,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    max(toDate(visits.emission_time)) as visited_on,
    pages.subsection_block_id as subsection_block_id,
    pages.section_block_id as section_block_id
from {{ ref("fact_learner_page_visits") }} visits
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        visits.org = blocks.org
        and visits.course_key = blocks.course_key
        and visits.block_id = blocks.block_id
    )
join
    {{ ref("dim_course_subsection_items") }} pages
    on (
        pages.org = visits.org
        and pages.course_key = visits.course_key
        and pages.section_number = blocks.section_number
        and pages.subsection_number = blocks.subsection_number
    )
where blocks.in_latest_publish and pages.block_type = 'unit'
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
