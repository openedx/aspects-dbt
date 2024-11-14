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
            and org in cast({org_filter:String}, 'Array(String)')
            and course_key in (
                select course_key
                from {{ ref("course_names") }}
                where course_name in cast({course_name_filter:String}, 'Array(String)')
            )
    ),
    matches as (
        select
            *,
            first_value(event_id) filter (where verb = 'start') over (
                partition by org, course_key, actor_id, object_id
                order by emission_time
                rows between unbounded preceding and 1 preceding
            ) as matching_event_id,
            first_value(event_id) filter (where verb = 'end') over (
                partition by org, course_key, actor_id, object_id
                order by emission_time
                rows between 1 following and unbounded following
            ) as matching_event_id2
        from data
    ),
    ends as (
        select *
        from matches
        where verb = 'end' and notEmpty(matching_event_id) and empty(matching_event_id2)
    ),
    starts as (
        select *
        from matches
        where
            verb = 'start' and notEmpty(matching_event_id2) and empty(matching_event_id)
    ),
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
        inner join ends on starts.event_id = ends.matching_event_id
        where ends.video_position > starts.video_position
    ),
    rewatched as (
        select a.event_id as event_id1, b.event_id as event_id2, actor_id
        from range a
        inner join
            range b
            on a.org = b.org
            and a.course_key = b.course_key
            and a.actor_id = b.actor_id
            and a.object_id = b.object_id
        where
            (
                (
                    b.start_position > a.start_position
                    and b.start_position < a.end_position
                )
                or (
                    b.end_position > a.start_position
                    and b.end_position < a.end_position
                )
            )
            and b.start_emission_time > a.start_emission_time
    ),
    rewatched_combined as (
        select event_id1 as event_id
        from rewatched
        union all
        select event_id2 as event_id
        from rewatched
    ),
    course_data as (
        select org, course_key, count(distinct block_id) video_count
        from {{ ref("dim_course_blocks") }}
        where block_type = 'video'
        group by org, course_key
    )
select
    concat(course_data.org, range.org) as org,
    concat(course_data.course_key, range.course_key) as course_key,
    range.actor_id as actor_id,
    video_duration,
    cast(video_count as Int32) as video_count,
    sum(
        case
            when empty(rewatched_combined.event_id)
            then end_position - start_position
            else 0
        end
    ) as watched_time,
    sum(
        case
            when notEmpty(rewatched_combined.event_id)
            then end_position - start_position
            else 0
        end
    ) as rewatched_time
from course_data
full join range on range.course_key = course_data.course_key
left join rewatched_combined on range.event_id = rewatched_combined.event_id
group by org, course_key, actor_id, video_count, video_duration
