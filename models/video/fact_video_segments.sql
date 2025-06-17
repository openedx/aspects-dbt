{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, object_id, actor_id, watched_segment, watch_count)",
        order_by="(org, course_key, object_id, actor_id, watched_segment, watch_count)",
    )
}}

with
    data as (
        select
            event_id,
            org,
            course_key,
            actor_id,
            emission_time_long,
            video_position,
            object_id,
            video_duration,
            if(
                verb_id = 'https://w3id.org/xapi/video/verbs/played', 'start', 'end'
            ) as verb
        from {{ ref("video_playback_events") }}
        where verb_id <> 'http://adlnet.gov/expapi/verbs/initialized'
    ),
    matches as (
        select
            *,
            first_value(event_id) filter (where verb = 'start') over (
                partition by org, course_key, actor_id, object_id
                order by emission_time_long, verb
                rows between 1 following and unbounded following
            ) as matching_event_id
        from data
    ),
    final_matches as (
        select
            *,
            last_value(event_id) over (
                partition by matching_event_id, object_id, actor_id
                order by emission_time_long
                rows between unbounded preceding and unbounded following
            ) as end_id
        from matches
        order by emission_time_long
    ),
    ends as (select * from final_matches where verb = 'end' and event_id = end_id),
    starts as (select * from final_matches where verb = 'start'),
    final_results as (
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
    )
select
    org,
    course_key,
    actor_id,
    object_id,
    video_duration,
    watched_segment,
    count(1) as watch_count
from final_results
group by org, course_key, actor_id, object_id, video_duration, watched_segment
