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
    ),
    final_results as (
        select
            plays.emission_time as emission_time,
            plays.org as org,
            plays.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            plays.video_id as video_id,
            blocks.block_name as video_name,
            blocks.display_name_with_location as video_name_with_location,
            {{ a_tag("plays.object_id", "blocks.display_name_with_location") }}
            as video_link,
            blocks.graded as graded,
            plays.video_position as video_position,
            plays.video_duration as video_duration,
            {{ get_bucket("video_position/video_duration") }} as visualization_bucket,
            plays.actor_id as actor_id,
            blocks.section_with_name as section_with_name,
            blocks.subsection_with_name as subsection_with_name,
            blocks.course_order as course_order
        from plays
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                plays.course_key = blocks.course_key
                and plays.video_id = blocks.block_id
            )
    )
select
    emission_time,
    org,
    course_key,
    course_name,
    course_run,
    -- video_id,
    -- video_name,
    video_name_with_location,
    video_link,
    -- graded,
    -- video_position,
    -- video_duration,
    -- visualization_bucket,
    actor_id,
    -- users.username as username,
    -- users.name as name,
    -- users.email as email,
    section_with_name,
    subsection_with_name,
    course_order
from final_results
left outer join
    {{ ref("dim_user_pii") }} users
    on (
        final_results.actor_id like 'mailto:%'
        and SUBSTRING(final_results.actor_id, 8) = users.email
    )
    or final_results.actor_id = toString(users.external_user_id)
