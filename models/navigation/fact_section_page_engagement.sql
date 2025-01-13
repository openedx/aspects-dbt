{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key)",
        order_by="(org, course_key, section_block_id, actor_id)",
    )
}}

with
    fact_navigation as (
        select
            navigation.emission_time as emission_time,
            navigation.org as org,
            navigation.course_key as course_key,
            navigation.actor_id as actor_id,
            navigation.block_id as block_id,
            blocks.display_name_with_location as block_name_with_location
        from {{ ref("navigation_events") }} navigation
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                navigation.course_key = blocks.course_key
                and navigation.block_id = blocks.block_id
            )
    ),
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
        from fact_navigation
    ),
    pages_per_subsection as (
        select * from ({{ items_per_subsection("%@vertical+block@%") }})
    ),
    fact_navigation_completion as (
        select
            visits.visited_on as visited_on,
            visits.org as org,
            visits.course_key as course_key,
            pages.section_with_name as section_with_name,
            pages.subsection_with_name as subsection_with_name,
            pages.item_count as item_count,
            visits.actor_id as actor_id,
            visits.block_id as block_id,
            pages.section_block_id as section_block_id
        from visited_subsection_pages visits
        join
            pages_per_subsection pages
            on (
                visits.org = pages.org
                and visits.course_key = pages.course_key
                and visits.section_number = pages.section_number
                and visits.subsection_number = pages.subsection_number
            )
    ),
    subsection_counts as (
        select
            org,
            course_key,
            section_with_name,
            subsection_with_name,
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
            section_with_name,
            subsection_with_name,
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
