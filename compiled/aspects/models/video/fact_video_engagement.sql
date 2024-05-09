with
    subsection_counts as (
        select
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            count(distinct video_id) as videos_viewed,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as engagement_level,
            username,
            name,
            email
        from `xapi`.`fact_video_engagement_per_subsection`
        group by
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            username,
            name,
            email
    ),
    section_counts as (
        select
            org,
            course_key,
            course_run,
            section_with_name,
            '' as subsection_with_name,
            actor_id,
            sum(item_count) as item_count,
            sum(videos_viewed) as videos_viewed,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as engagement_level,
            username,
            name,
            email
        from subsection_counts
        group by
            org,
            course_key,
            course_run,
            section_with_name,
            subsection_with_name,
            actor_id,
            username,
            name,
            email
    ),
    all_counts as (

        select
            org,
            course_key,
            course_run,
            section_with_name as section_with_name,
            subsection_with_name as subsection_with_name,
            subsection_with_name as section_subsection_name,
            'subsection' as content_level,
            actor_id as actor_id,
            engagement_level as section_subsection_video_engagement,
            username,
            name,
            email
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
            engagement_level as section_subsection_video_engagement,
            username,
            name,
            email
        from section_counts
    )
select
    ac.org as org,
    ac.course_key as course_key,
    ac.course_run as course_run,
    ac.section_with_name as section_with_name,
    ac.subsection_with_name as subsection_with_name,
    ac.section_subsection_name as section_subsection_name,
    ac.content_level as content_level,
    ac.actor_id as actor_id,
    ac.section_subsection_video_engagement as section_subsection_video_engagement,
    ac.username as username,
    ac.name as name,
    ac.email as email
from all_counts ac