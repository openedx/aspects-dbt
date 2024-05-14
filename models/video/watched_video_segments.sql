{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, video_id)",
        order_by="(org, course_key, video_id, actor_id, started_at, end_type, started_at, ended_at, start_position, end_position, video_duration)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

with
    video_events as (
        select
            emission_time,
            org,
            course_key,
            video_id,
            actor_id,
            verb_id,
            video_position,
            video_duration
        from {{ ref("video_playback_events") }}
    ),
    starts as (
        select *
        from video_events
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    ),
    ends as (
        select *
        from video_events
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/seeked',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated'
            )
    ),
    segments as (
        select
            starts.org as org,
            starts.course_key as course_key,
            starts.video_id as video_id,
            starts.actor_id,
            cast(starts.video_position as Int32) as start_position,
            cast(ends.video_position as Int32) as end_position,
            starts.emission_time as started_at,
            ends.emission_time as ended_at,
            ends.verb_id as end_type,
            starts.video_duration as video_duration
        from starts left
        asof join
            ends
            on (
                starts.org = ends.org
                and starts.course_key = ends.course_key
                and starts.video_id = ends.video_id
                and starts.actor_id = ends.actor_id
                and starts.emission_time < ends.emission_time
            )
    )
select
    org,
    course_key,
    video_id,
    actor_id,
    start_position,
    end_position,
    started_at,
    ended_at,
    end_type,
    video_duration,
    ended_at as emission_time
from segments
