with
    segments as (
        select
            *
        from {{ ref("watched_video_segments") }}
    ),
    enriched_segments as (
        select
            segments.org as org,
            segments.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            blocks.section_with_name as section_with_name,
            blocks.subsection_with_name as subsection_with_name,
            blocks.block_name as video_name,
            blocks.display_name_with_location as video_name_with_location,
            segments.actor_id as actor_id,
            segments.started_at as started_at,
            segments.start_position - (segments.start_position % 5) as start_position,
            segments.end_position - (segments.end_position % 5) as end_position,
            segments.video_duration as video_duration
        from segments
        join
            {{ ref("dim_course_blocks_extended") }} blocks
            on (
                segments.course_key = blocks.course_key
                and segments.video_id = blocks.block_id
            )
    )

select
    org,
    course_key,
    course_name,
    course_run,
    section_with_name,
    subsection_with_name,
    video_name,
    video_name_with_location,
    actor_id,
    started_at,
    arrayJoin(range(start_position, end_position, 5)) as segment_start,
    video_duration,
    CONCAT(toString(segment_start), '-', toString(segment_start + 4)) as segment_range,
    start_position,
    username,
    name,
    email
from enriched_segments
left outer join
    {{ ref("dim_user_pii") }} users on toUUID(actor_id) = users.external_user_id
order by start_position
