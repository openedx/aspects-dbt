with
    watched_segments as (
        select *
        from {{ ref("fact_video_segments") }}
        where
            (
                (
                    {org_filter:String} <> '[]'
                    and org in cast({org_filter:String}, 'Array(String)')
                )
                or {org_filter:String} = '[]'
            )
            and (
                (
                    {course_key_filter:String} <> '[]'
                    and course_key in cast({course_key_filter:String}, 'Array(String)')
                )
                or {course_key_filter:String} = '[]'
            )
    ),
    rewatched as (
        select org, course_key, actor_id, object_id, video_duration, watched_segment
        from watched_segments
        group by org, course_key, actor_id, object_id, video_duration, watched_segment
        having count(1) > 1
    ),
    watched as (
        select org, course_key, actor_id, object_id, video_duration, watched_segment
        from watched_segments
        where
            (org, course_key, actor_id, object_id, watched_segment) not in (
                select org, course_key, actor_id, object_id, watched_segment
                from rewatched
            )
    ),
    watched_combined as (
        select
            org,
            course_key,
            actor_id,
            object_id,
            video_duration,
            watched_segment as watched_segment_combine,
            0 as rewatched_segment
        from watched
        union all
        select
            org,
            course_key,
            actor_id,
            object_id,
            video_duration,
            0 as watched_segment_combine,
            watched_segment as rewatched_segment
        from rewatched
    )
select
    org,
    course_key,
    actor_id,
    object_id,
    video_duration,
    watched_segment_combine as watched_segment,
    rewatched_segment
from watched_combined
where watched_segment <> 0 or rewatched_segment <> 0
