with
    visited_subsection_pages as (
        select distinct
            date(navigation.emission_time) as visited_on,
            navigation.org as org,
            navigation.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            {{ section_from_display("blocks.display_name_with_location") }}
            as section_number,
            {{ subsection_from_display("blocks.display_name_with_location") }}
            as subsection_number,
            navigation.actor_id as actor_id,
            navigation.block_id as block_id,
            users.username as username,
            users.name as name,
            users.email as email
        from {{ ref("navigation_events") }} navigation
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                navigation.course_key = blocks.course_key
                and navigation.block_id = blocks.block_id
            )
        left outer join
            {{ ref("dim_user_pii") }} users
            on (actor_id like 'mailto:%' and SUBSTRING(actor_id, 8) = users.email)
            or actor_id = toString(users.external_user_id)
    ),
    pages as (
        select
            org,
            course_key,
            section_number,
            subsection_number,
            section_with_name,
            subsection_with_name,
            course_order,
            count(pages.original_block_id) as item_count
        from {{ ref("items_per_subsection") }} pages
        where pages.original_block_id like '%@vertical+block@%'
        group by
            section_with_name,
            subsection_with_name,
            course_order,
            org,
            course_key,
            section_number,
            subsection_number
    )
select
    visits.visited_on as visited_on,
    visits.org as org,
    visits.course_key as course_key,
    visits.course_name as course_name,
    visits.course_run as course_run,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    pages.course_order as course_order,
    pages.item_count as item_count,
    visits.actor_id as actor_id,
    visits.block_id as block_id,
    visits.username as username,
    visits.name as name,
    visits.email as email
from visited_subsection_pages visits
join
    pages
    on (
        visits.org = pages.org
        and visits.course_key = pages.course_key
        and visits.section_number = pages.section_number
        and visits.subsection_number = pages.subsection_number
    )
