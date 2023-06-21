-- model to support number of watches per video
-- ref: https://edx.readthedocs.io/projects/edx-insights/en/latest/Reference.html#engagement-computations

select
    emission_time,
    org,
    course_id,
    splitByString('/xblock/', object_id)[2] as video_id,
    actor_id
from
    {{ source('xapi', 'video_playback_events') }}
where
    event_type = 'played'
