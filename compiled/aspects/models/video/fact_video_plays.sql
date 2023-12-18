-- model to support number of watches per video
-- ref: https://edx.readthedocs.io/projects/edx-insights/en/latest/Reference.html#engagement-computations
with plays as (
    select
        emission_time,
        org,
        course_key,
        splitByString('/xblock/', object_id)[-1] as video_id,
        actor_id
    from
        `xapi`.`video_playback_events`
    where
        verb_id = 'https://w3id.org/xapi/video/verbs/played'
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
    plays.actor_id as actor_id
from
    plays
    join `xapi`.`dim_course_blocks` blocks
         on (plays.course_key = blocks.course_key
             and plays.video_id = blocks.block_id)