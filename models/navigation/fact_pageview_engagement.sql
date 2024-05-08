with
    subsection_counts as (
        select
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id,
            page_count,
            count(distinct block_id) as pages_visited,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = page_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as engagement_level
        from {{ ref("fact_navigation_completion") }}
        group by
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id,
            page_count
    ),
    section_counts as (
        select
            org,
            course_key,
            course_run,
            section_with_name,
            '' as subsection_with_name,
            actor_id,
            sum(page_count) as page_count,
            sum(pages_visited) as pages_visited,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = page_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as engagement_level
        from subsection_counts
        group by
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id
    ),
    pageview_counts as (
        select
            org,
            course_key,
            course_run,
            section_with_name as section_with_name,
            subsection_with_name as subsection_with_name,
            subsection_with_name as section_subsection_name,
            'subsection' as content_level,
            actor_id as actor_id,
            engagement_level as section_subsection_page_engagement
        from subsection_counts
        union all
        select
            org,
            course_key,
            course_run,
            section_with_name as section_with_name,
            subsection_with_name as subsection_with_name,
            section_with_name as section_subsection_name,
            'section' as content_level,
            actor_id as actor_id,
            engagement_level as section_subsection_page_engagement
        from section_counts

    )

select
    pv.org as org,
    pv.course_key as course_key,
    pv.course_run as course_run,
    pv.section_with_name as section_with_name,
    pv.subsection_with_name as subsection_with_name,
    pv.section_subsection_name as section_subsection_name,
    pv.content_level as content_level,
    pv.actor_id as actor_id,
    pv.section_subsection_page_engagement as section_subsection_page_engagement,
    users.username as username,
    users.name as name,
    users.email as email
from pageview_counts pv
left outer join
    {{ ref("dim_user_pii") }} users on toUUID(actor_id) = users.external_user_id
