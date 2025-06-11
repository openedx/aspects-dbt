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
    segments as (
        select
            event_id,
            org,
            course_key,
            actor_id,
            object_id,
            video_duration,
            watched_segment,
            case
                when
                    (org, course_key, actor_id, object_id, watched_segment) in (
                        select org, course_key, actor_id, object_id, watched_segment
                        from rewatched
                    )
                then 1
                else 0
            end as rewatched
        from watched_segments
    )
select
    segments.event_id,
    segments.org,
    segments.course_key,
    segments.actor_id,
    segments.object_id,
    segments.video_duration,
    segments.watched_segment,
    segments.rewatched,
    formatDateTime(
        toDate(now()) + toIntervalSecond(segments.watched_segment), '%T'
    ) as time_stamp,
    arrayStringConcat(
        arrayMap(
            x -> (leftPad(x, 2, char(917768))),
            splitByString(
                ':', splitByString(' - ', blocks.display_name_with_location)[1]
            )
        ),
        ':'
    ) as video_number,
    concat(
        video_number, ' - ', splitByString(' - ', blocks.display_name_with_location)[2]
    ) as video_name_location,
    concat(
        '<a href="',
        segments.object_id,
        '" target="_blank">',
        video_name_location,
        '</a>'
    ) as video_link,
    blocks.section_with_name as section_with_name,
    blocks.subsection_with_name as subsection_with_name
from segments
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        segments.course_key = blocks.course_key
        and splitByString('/xblock/', segments.object_id)[-1] = blocks.block_id
    )
