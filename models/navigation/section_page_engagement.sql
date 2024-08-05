{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, section_block_id, actor_id)",
    )
}}

with
    visited_subsection_pages as (
        select distinct
            date(emission_time) as visited_on,
            org,
            course_key,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            block_id
        from {{ ref("fact_navigation") }}
    ),
    fact_navigation_completion as (
        select
            visits.visited_on as visited_on,
            visits.org as org,
            visits.course_key as course_key,
            pages.section_block_id as section_block_id,
            pages.section_name_with_location as section_name_with_location,
            pages.subsection_block_id as subsection_block_id,
            pages.subsection_name_with_location as subsection_name_with_location,
            pages.item_count as item_count,
            visits.actor_id as actor_id,
            visits.block_id as block_id,
            pages.section_block_id as section_block_id
        from visited_subsection_pages visits
        join
            {{ ref("int_pages_per_subsection") }} pages
            on (
                visits.org = pages.org
                and visits.course_key = pages.course_key
                and visits.section_name_with_location = pages.section_name_with_location
                and visits.subsection_name_with_location
                = pages.subsection_name_with_location
            )
    ),
    subsection_counts as (
        select
            org,
            course_key,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            item_count,
            count(distinct block_id) as pages_visited,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = item_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as engagement_level,
            section_block_id
        from fact_navigation_completion
        group by
            org,
            course_key,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            item_count,
            section_block_id
    ),
    section_counts as (
        select
            org,
            course_key,
            actor_id,
            sum(item_count) as item_count,
            sum(pages_visited) as pages_visited,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = item_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as engagement_level,
            section_block_id
        from subsection_counts
        group by org, course_key, section_block_id, actor_id
    )

select org, course_key, actor_id, section_block_id, engagement_level
from section_counts
