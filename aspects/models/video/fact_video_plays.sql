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
        {{ source('xapi', 'video_playback_events') }}
    where
        verb_id = 'https://w3id.org/xapi/video/verbs/played'
)

select
    plays.emission_time as emission_time,
    plays.org as org,
    courses.course_name as course_name,
    courses.course_run as course_run,
    blocks.block_name as video_name,
    plays.actor_id as actor_id
from
    plays
    join {{ source('event_sink', 'course_names')}} courses
         on plays.course_key = courses.course_key
    join {{ source('event_sink', 'course_block_names')}} blocks
         on plays.video_id = blocks.location
