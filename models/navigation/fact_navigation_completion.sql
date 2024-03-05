-- number of learners who've viewed all pages in a section/subsection
with
    visited_subsection_pages as (
        select distinct
            date(emission_time) as visited_on,
            org,
            course_key,
            {{ section_from_display("block_name_with_location") }} as section_number,
            {{ subsection_from_display("block_name_with_location") }}
            as subsection_number,
            actor_id,
            block_id
        from {{ ref("fact_navigation") }}
    )

select
    visits.visited_on,
    visits.org,
    visits.course_key,
    pages.section_with_name,
    pages.subsection_with_name,
    pages.page_count,
    visits.actor_id,
    visits.block_id
from visited_subsection_pages visits
join
    {{ ref("int_pages_per_subsection") }} pages
    on (
        visits.org = pages.org
        and visits.course_key = pages.course_key
        and visits.section_number = pages.section_number
        and visits.subsection_number = pages.subsection_number
    )
