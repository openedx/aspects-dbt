

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
        from `xapi`.`video_playback_events`
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
        from `xapi`.`video_playback_events`
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
            ends.emission_time as end_emission_time,
            row_number() over (
                partition by org, course_key, actor_id, video_id, start_position
                order by ends.emission_time
            ) as rownum
        from starts
        left join
            ends
            on starts.org = ends.org
            and starts.course_key = ends.course_key
            and starts.video_id = ends.video_id
            and starts.actor_id = ends.actor_id
        where
            starts.emission_time < ends.emission_time
            and starts.start_position < ends.end_position
    ),
    range as (select * from range_multi where rownum = 1),
    rewatched as (
        select a.event_id as event_id1, b.event_id as event_id2, actor_id
        from range a
        inner join
            range b
            on a.org = b.org
            and a.course_key = b.course_key
            and a.actor_id = b.actor_id
            and a.video_id = b.video_id
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
    course_data as (
        select org, course_key, count(distinct block_id) video_count
        from `xapi`.`dim_course_blocks`
        where block_type = 'video'
        group by org, course_key
    )
select
    course_data.org as org,
    course_data.course_key as course_key,
    range.actor_id as actor_id,
    video_duration,
    cast(video_count as Int32) as video_count,
    sum(
        case
            when r1.actor_id = '' and r2.actor_id = ''
            then end_position - start_position
            else 0
        end
    ) as watched_time,
    sum(
        case
            when r1.actor_id <> '' or r2.actor_id <> ''
            then end_position - start_position
            else 0
        end
    ) as rewatched_time
from course_data
left join range on range.course_key = course_data.course_key
left join rewatched r1 on range.event_id = r1.event_id1
left join rewatched r2 on range.event_id = r2.event_id2
group by org, course_key, actor_id, video_count, video_duration