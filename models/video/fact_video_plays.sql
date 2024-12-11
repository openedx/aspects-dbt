-- model to support number of watches per video
-- ref:
-- https://edx.readthedocs.io/projects/edx-insights/en/latest/Reference.html#engagement-computations
with
    plays as (
        select
            emission_time,
            org,
            course_key,
            object_id,
            video_duration,
            video_position,
            splitByString('/xblock/', object_id)[-1] as video_id,
            actor_id
        from {{ ref("video_playback_events") }}
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
        {{ common_filters() }}
    )

select
    plays.emission_time as emission_time,
    plays.org as org,
    plays.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    plays.video_id as video_id,
    blocks.block_name as video_name,
    blocks.display_name_with_location as video_name_with_location,
    {{ a_tag("plays.object_id", "blocks.display_name_with_location") }} as video_link,
    blocks.graded as graded,
    plays.video_position as video_position,
    plays.video_duration as video_duration,
    {{ get_bucket("video_position/video_duration") }} as visualization_bucket,
    plays.actor_id as actor_id,
    users.username as username,
    users.name as name,
    users.email as email,
    blocks.section_with_name as section_with_name,
    blocks.subsection_with_name as subsection_with_name,
    blocks.course_order as course_order
from plays
join
    {{ ref("dim_course_blocks_extended") }} blocks
    on (plays.course_key = blocks.course_key and plays.video_id = blocks.block_id)
left outer join
    {{ ref("dim_user_pii") }} users
    on (actor_id like 'mailto:%' and SUBSTRING(actor_id, 8) = users.email)
    or actor_id = toString(users.external_user_id)
