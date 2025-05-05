with
    pageview_engagement as (
        select
            org,
            course_key,
            course_order,
            actor_id,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = item_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as section_subsection_page_engagement,
            block_id,
            section_subsection_name,
            content_level
        from {{ ref("fact_navigation_section_subsection") }} ARRAY
        join
            arrayConcat([subsection_block_id], [section_block_id]) as block_id,
            arrayConcat(
                [subsection_with_name], [section_with_name]
            ) as section_subsection_name,
            arrayConcat(
                [subsection_content_level], [section_content_level]
            ) as content_level
        group by
            org,
            course_key,
            course_order,
            actor_id,
            section_subsection_page_engagement,
            block_id,
            section_subsection_name,
            content_level
    ),
    final_results as (
        select distinct
            pageview_engagement.org as org,
            pageview_engagement.course_key as course_key,
            pageview_engagement.section_subsection_name as section_subsection_name,
            pageview_engagement.content_level as content_level,
            pageview_engagement.actor_id as actor_id,
            pageview_engagement.section_subsection_page_engagement
            as section_subsection_page_engagement,
            users.username as username,
            users.name as name,
            users.email as email
        from pageview_engagement
        left outer join
            {{ ref("dim_user_pii") }} users
            on (
                pageview_engagement.actor_id like 'mailto:%'
                and SUBSTRING(pageview_engagement.actor_id, 8) = users.email
            )
            or pageview_engagement.actor_id = toString(users.external_user_id)
        where section_subsection_name <> ''
    )
select *
from final_results
