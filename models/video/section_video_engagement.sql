{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, section_block_id, actor_id)",
    )
}}

with
    viewed_subsection_videos as (
        select distinct
            date(plays.emission_time) as viewed_on,
            plays.org,
            plays.course_key,
            plays.actor_id,
            plays.video_id,
            blocks.section_block_id,
            blocks.subsection_block_id
        from {{ ref("video_play_events") }} plays
        join {{ ref("dim_course_blocks") }} blocks
            on plays.video_id = blocks.block_id
    ),
    fact_video_engagement_per_section as (
        select
            views.org as org,
            views.course_key as course_key,
            views.actor_id as actor_id,
            views.video_id as video_id,
            videos.section_block_id as section_block_id,
            videos.item_count as item_count
        from viewed_subsection_videos views
        join
            {{ ref("int_videos_per_subsection") }} videos
            on views.section_block_id = videos.section_block_id
    ),
    section_counts as (
        select
            org,
            course_key,
            section_block_id,
            actor_id,
            sum(item_count) as item_count,
            count(distinct video_id) as videos_viewed,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as engagement_level
        from fact_video_engagement_per_section
        group by org, course_key, section_block_id, actor_id
    )
select org, course_key, section_block_id, actor_id, engagement_level
from section_counts
