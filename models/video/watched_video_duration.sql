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
            {{ common_filters() }}
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
    starts as (select * from final_matches where verb = 'start'),
    range as (
        select
            starts.event_id,
            starts.org,
            starts.course_key,
            starts.actor_id,
            starts.object_id,
            starts.video_duration,
            starts.video_position as start_position,
            ends.video_position as end_position,
            starts.emission_time as start_emission_time,
            ends.emission_time as end_emission_time
        from starts
        inner join ends on starts.end_id = ends.event_id
        where ends.video_position > starts.video_position
    ),
    rewatched as (
        select
            a.org,
            a.course_key,
            a.actor_id,
            a.object_id,
            a.video_duration,
            max(a.end_position) - min(a.start_position) as watched_time,
            max(b.end_position) - min(b.start_position) as rewatched_time
        from range a
        inner join
            range b
            on a.org = b.org
            and a.course_key = b.course_key
            and a.actor_id = b.actor_id
            and a.object_id = b.object_id
        where
            a.event_id <> b.event_id
            and (
                (
                    b.start_position > a.start_position
                    and b.start_position < a.end_position
                )
                or (
                    b.end_position > a.start_position
                    and b.end_position < a.end_position
                )
            )
        group by a.org, a.course_key, a.actor_id, a.object_id, a.video_duration
    ),
    watched as (
        select
            org,
            course_key,
            actor_id,
            object_id,
            video_duration,
            end_position - start_position as watched_time,
            0 as rewatched_time
        from range
        where
            (org, course_key, actor_id, object_id)
            not in (select org, course_key, actor_id, object_id from rewatched)
    ),
    watched_combined as (
        select *
        from watched
        union all
        select *
        from rewatched
    ),
    course_data as (
        select org, course_key, count(distinct block_id) video_count
        from {{ ref("dim_course_blocks") }}
        where block_type = 'video'
        group by org, course_key
    )
select
    coalesce(nullIf(course_data.org, ''), watched_combined.org) as org,
    coalesce(
        nullIf(course_data.course_key, ''), watched_combined.course_key
    ) as course_key,
    actor_id,
    video_duration,
    cast(video_count as Int32) as video_count,
    sum(watched_time) as watched_time,
    sum(rewatched_time) as rewatched_time,
    object_id
from course_data
full join watched_combined on watched_combined.course_key = course_data.course_key
group by org, course_key, actor_id, video_count, video_duration, object_id
