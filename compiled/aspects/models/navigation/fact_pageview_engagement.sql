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
        from `xapi`.`fact_navigation_completion`
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
    pageview_engagement.org as org,
    pageview_engagement.course_key as course_key,
    pageview_engagement.section_subsection_name as section_subsection_name,
    pageview_engagement.content_level as content_level,
    pageview_engagement.actor_id as actor_id,
    pageview_engagement.section_subsection_page_engagement
    as section_subsection_page_engagement,
    pageview_engagement.section_with_name as section_with_name,
    users.username as username,
    users.name as name,
    users.email as email
from pageview_engagement
left outer join
    `xapi`.`dim_user_pii` users
    on (
        pageview_engagement.actor_id like 'mailto:%'
        and SUBSTRING(pageview_engagement.actor_id, 8) = users.email
    )
    or pageview_engagement.actor_id = toString(users.external_user_id)
where pageview_engagement.section_subsection_name <> ''