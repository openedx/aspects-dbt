{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key)",
        order_by="(org, course_key, subsection_block_id, actor_id)",
    )
}}

with
    plays as (
        select
            org,
            course_key,
            splitByString('/xblock/', object_id)[-1] as video_id,
            actor_id
        from {{ ref("video_playback_events") }}
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    ),
    fact_video_plays as (
        select distinct
            plays.org as org,
            plays.course_key as course_key,
            blocks.section_number as section_number,
            blocks.subsection_number as subsection_number,
            plays.actor_id as actor_id
        from plays
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                plays.course_key = blocks.course_key
                and plays.video_id = blocks.block_id
            )
    ),
    fact_video_engagement_per_subsection as (
        select
            views.org as org,
            views.course_key as course_key,
            count(videos.original_block_id) as item_count,
            views.actor_id as actor_id,
            videos.subsection_block_id as subsection_block_id
        from fact_video_plays views
        join
            {{ ref('items_per_subsection') }} videos
            on (
                views.org = videos.org
                and views.course_key = videos.course_key
                and views.section_number = videos.section_number
                and views.subsection_number = videos.subsection_number
                and videos.original_block_id like '%@video+block@%'
            )
    )
    select distinct
        org,
        course_key,
        actor_id,
        case
            when videos_viewed = 0
            then 'No videos viewed yet'
            when videos_viewed = item_count
            then 'All videos viewed'
            else 'At least one video viewed'
        end as engagement_level,
        subsection_block_id
    from fact_video_engagement_per_subsection
