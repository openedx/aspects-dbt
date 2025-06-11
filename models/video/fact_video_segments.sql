{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, object_id, actor_id, watched_segment, event_id)",
        order_by="(org, course_key, object_id, actor_id, watched_segment, event_id)",
    )
}}

with
    data as (
        select
            event_id,
            org,
            course_key,
            actor_id,
            emission_time,
            video_position,
            object_id,
            video_duration,
            if(
                verb_id = 'https://w3id.org/xapi/video/verbs/played', 'start', 'end'
            ) as verb
        from {{ ref("video_playback_events") }}
        where
            verb_id in (
                'https://w3id.org/xapi/video/verbs/played',
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated',
                'https://w3id.org/xapi/video/verbs/seeked'
            )
    ),
    matches as (
        select
            *,
            first_value(event_id) filter (where verb = 'start') over (
                partition by org, course_key, actor_id, object_id
                order by emission_time, verb
                rows between 1 following and unbounded following
            ) as matching_event_id
        from data
    ),
    final_matches as (
        select
            *,
            last_value(event_id) over (
                partition by matching_event_id, object_id, actor_id
                order by emission_time
                rows between unbounded preceding and unbounded following
            ) as end_id
        from matches
        order by emission_time
    ),
    ends as (select * from final_matches where verb = 'end' and event_id = end_id),
    starts as (select * from final_matches where verb = 'start')
select
    starts.event_id as event_id,
    starts.org as org,
    starts.course_key as course_key,
    starts.actor_id as actor_id,
    starts.object_id as object_id,
    starts.video_duration as video_duration,
    arrayJoin(
        range(
            cast(starts.video_position as int) + 1,
            cast(ends.video_position as int) + 1,
            1
        )
    ) as watched_segment
from starts
inner join ends on starts.end_id = ends.event_id
where ends.video_position > starts.video_position
