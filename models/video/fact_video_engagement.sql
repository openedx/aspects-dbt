{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
    )
}}

with
    video_engagment as (
        select
            org,
            course_key,
            actor_id,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as section_subsection_video_engagement,
            block_id,
            section_subsection_name,
            content_level
        from {{ ref("fact_video_section_subsection") }} ARRAY
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
            actor_id,
            section_subsection_video_engagement,
            block_id,
            section_subsection_name,
            content_level
    ),
    final_results as (
        select
            video_engagment.org as org,
            video_engagment.course_key as course_key,
            video_engagment.section_subsection_name as section_subsection_name,
            video_engagment.content_level as content_level,
            video_engagment.actor_id as actor_id,
            video_engagment.section_subsection_video_engagement
            as section_subsection_video_engagement,
            users.username as username,
            users.name as name,
            users.email as email
        from video_engagment
        left outer join
            {{ ref("dim_user_pii") }} users
            on (
                video_engagment.actor_id like 'mailto:%'
                and SUBSTRING(video_engagment.actor_id, 8) = users.email
            )
            or video_engagment.actor_id = toString(users.external_user_id)
        where section_subsection_name <> ''
    )
select *
from final_results
