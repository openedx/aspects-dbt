with
    course_data as (
        select org, course_key, count(distinct block_id) video_count
        from {{ ref("dim_course_blocks") }}
        where block_type = 'video'
        group by org, course_key
    ),
    watches as (
        select *
        from
            {{ ref("fact_video_segment_watches") }} (
                org_filter = {org_filter:String},
                course_key_filter = {course_key_filter:String}
            )
    )
select
    coalesce(nullIf(course_data.org, ''), watches.org) as org,
    coalesce(nullIf(course_data.course_key, ''), watches.course_key) as course_key,
    watches.actor_id,
    watches.video_duration,
    cast(video_count as Int32) as video_count,
    sum(
        case when coalesce(watches.watched_segment, 0) > 0 then 1 else 0 end
    ) as watched_time,
    sum(
        case when coalesce(watches.rewatched_segment, 0) > 0 then 1 else 0 end
    ) as rewatched_time,
    watches.object_id
from course_data
full join watches on watches.course_key = course_data.course_key
group by org, course_key, actor_id, video_count, video_duration, object_id
