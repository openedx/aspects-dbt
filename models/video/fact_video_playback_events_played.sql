with
    get_video_id as (
        select
            plays.org as org,
            plays.course_key as course_key,
            blocks.course_order as course_order,
            blocks.section_number as section_number,
            blocks.subsection_number as subsection_number,
            plays.actor_id as actor_id,
            splitByString('/xblock/', plays.object_id)[-1] as _video_id
        from {{ ref("video_playback_events") }} plays
        join
            {{ ref("dim_course_blocks") }} blocks
            on (plays.course_key = blocks.course_key and _video_id = blocks.block_id)
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    )
select
    org,
    course_key,
    course_order,
    section_number,
    subsection_number,
    actor_id,
    count(distinct _video_id) as videos_viewed
from get_video_id
group by org, course_key, course_order, section_number, subsection_number, actor_id
