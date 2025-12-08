{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, block_id, actor_id)",
        order_by="(org, course_key, block_id, actor_id)",
    )
}}

with
    pageview_section_subsection as (
        select
            org,
            course_key,
            actor_id,
            'section' as section_content_level,
            'subsection' as subsection_content_level,
            page_count,
            count(block_id) as pages_visited,
            section_with_name,
            subsection_with_name,
            subsection_block_id,
            section_block_id
        from {{ ref("fact_navigation_completion") }}
        group by
            org,
            course_key,
            actor_id,
            page_count,
            section_with_name,
            subsection_with_name,
            subsection_block_id,
            section_block_id
    ),
    pageview_engagement as (
        select
            org,
            course_key,
            actor_id,
            sum(pages_visited) as pages_visited,
            sum(page_count) as page_count,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = page_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as section_subsection_page_engagement,
            section_with_name,
            section_subsection_name,
            content_level,
            block_id
        from pageview_section_subsection ARRAY
        join
            arrayConcat(
                [subsection_with_name], [section_with_name]
            ) as section_subsection_name,
            arrayConcat(
                [subsection_content_level], [section_content_level]
            ) as content_level,
            arrayConcat([subsection_block_id], [section_block_id]) as block_id
        group by
            org,
            course_key,
            actor_id,
            section_subsection_name,
            section_with_name,
            content_level,
            block_id
    )
select
    org,
    course_key,
    section_subsection_name,
    content_level,
    actor_id,
    section_with_name,
    block_id,
    section_subsection_page_engagement
from pageview_engagement
where section_subsection_name <> ''
