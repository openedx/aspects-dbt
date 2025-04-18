{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
    )
}}

with
    video_plays as (
        select
            org,
            course_key,
            splitByString('/xblock/', object_id)[-1] as video_id,
            actor_id
        from {{ ref("video_playback_events") }}
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    ),
    video_blocks as (
        select
            plays.org as org,
            plays.course_key as course_key,
            blocks.course_run as course_run,
            blocks.section_number as section_number,
            blocks.subsection_number as subsection_number,
            plays.actor_id as actor_id,
            count(distinct video_id) as videos_viewed
        from video_plays plays
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                plays.course_key = blocks.course_key
                and plays.video_id = blocks.block_id
            )
        group by
            org, course_key, course_run, section_number, subsection_number, actor_id
    ),
    section_subsection_blocks as (
        select
            views.org as org,
            views.course_key as course_key,
            views.course_run as course_run,
            count(original_block_id) as item_count,
            sum(views.videos_viewed) as videos_viewed,
            views.actor_id as actor_id,
            videos.subsection_block_id as subsection_block_id,
            videos.section_block_id as section_block_id,
            videos.subsection_with_name as subsection_with_name,
            videos.section_with_name as section_with_name,
            'section' as section_content_level,
            'subsection' as subsection_content_level
        from {{ ref("items_per_subsection") }} views
        join
            videos
            on (
                views.org = videos.org
                and views.course_key = videos.course_key
                and views.section_number = videos.section_number
                and views.subsection_number = videos.subsection_number
            )
        where original_block_id like '%@video+block@%'
        group by
            org,
            course_key,
            actor_id,
            subsection_block_id,
            section_block_id,
            subsection_with_name,
            section_with_name,
            course_run
    ),
    combined as (
        select
            org,
            course_key,
            course_run,
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
        from section_subsection_blocks ARRAY
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
            course_run,
            actor_id,
            section_subsection_video_engagement,
            block_id,
            section_subsection_name,
            content_level
    ),
    final_results as (
        select
            ve.org as org,
            ve.course_key as course_key,
            ve.course_run as course_run,
            ve.section_subsection_name as section_subsection_name,
            ve.content_level as content_level,
            ve.actor_id as actor_id,
            ve.section_subsection_video_engagement
            as section_subsection_video_engagement,
            users.username as username,
            users.name as name,
            users.email as email
        from combined ve
        left outer join
            {{ ref("dim_user_pii") }} users
            on (ve.actor_id like 'mailto:%' and SUBSTRING(ve.actor_id, 8) = users.email)
            or ve.actor_id = toString(users.external_user_id)
        where section_subsection_name <> ''
    )
select *
from final_results
