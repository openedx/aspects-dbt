-- number of learners who've viewed all pages in a section/subsection
with
    visited_subsection_pages as (
        select distinct
            date(emission_time) as visited_on,
            org,
            course_key,
            course_name,
            course_run,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            block_id
        from {{ ref("fact_navigation") }}
    )

select
    visits.visited_on as visited_on,
    visits.org as org,
    visits.course_key as course_key,
    visits.course_name as course_name,
    visits.course_run as course_run,
    pages.section_name_with_location as section_name_with_location,
    pages.subsection_name_with_location as subsection_name_with_location,
    pages.course_order as course_order,
    pages.item_count as page_count,
    visits.actor_id as actor_id,
    visits.block_id as block_id
from visited_subsection_pages visits
join
    {{ ref("int_pages_per_subsection") }} pages
    on (
        visits.org = pages.org
        and visits.course_key = pages.course_key
        and visits.section_name_with_location = pages.section_name_with_location
        and visits.subsection_name_with_location = pages.subsection_name_with_location
    )
