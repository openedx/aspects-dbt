{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org,course_key,video_id,actor_id,start_position,end_position,start_emission_time)",
        primary_key="(org,course_key,video_id,actor_id)",
    )
}}

with
    starts as (
        select
            event_id,
            org,
            course_key,
            actor_id,
            emission_time,
            cast(video_position as Int32) as start_position,
            splitByString('/xblock/', object_id)[-1] as video_id,
            video_duration
        from {{ ref("video_playback_events") }}
        where verb_id in ('https://w3id.org/xapi/video/verbs/played')
    ),
    ends as (
        select
            org,
            course_key,
            actor_id,
            emission_time,
            cast(video_position as Int32) as end_position,
            splitByString('/xblock/', object_id)[-1] as video_id
        from {{ ref("video_playback_events") }}
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated',
                'https://w3id.org/xapi/video/verbs/seeked'
            )
    ),
    range_multi as (
        select
            starts.event_id as event_id,
            starts.org as org,
            starts.course_key as course_key,
            starts.actor_id as actor_id,
            starts.video_id as video_id,
            starts.video_duration as video_duration,
            starts.start_position as start_position,
            ends.end_position as end_position,
            starts.emission_time as start_emission_time,
            row_number() over (
                partition by org, course_key, actor_id, video_id, start_position
                order by ends.emission_time
            ) as rownum
        from starts
        inner join ends
            on starts.org = ends.org
            and starts.course_key = ends.course_key
            and starts.video_id = ends.video_id
            and starts.actor_id = ends.actor_id
        where
            starts.emission_time < ends.emission_time
            and starts.start_position < ends.end_position
    )
    select * except (rownum) from range_multi where rownum = 1
