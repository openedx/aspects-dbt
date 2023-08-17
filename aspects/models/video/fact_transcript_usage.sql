with transcripts as (
    select
        emission_time,
        org,
        splitByString('/', course_id)[-1] as course_key,
        splitByString('/xblock/', object_id)[2] as video_id,
        actor_id
    from
        {{ source('xapi', 'xapi_events_all_parsed') }}
    where
        verb_id = 'http://adlnet.gov/expapi/verbs/interacted'
        and JSON_VALUE(event_str, '$.result.extensions."https://w3id.org/xapi/video/extensions/cc-enabled"') = 'true'
)

select
    transcripts.emission_time as emission_time,
    transcripts.org as org,
    courses.course_name as course_name,
    courses.course_run as course_run,
    blocks.block_name as video_name,
    transcripts.actor_id as actor_id
from
    transcripts
    join {{ source('event_sink', 'course_names')}} courses
         on transcripts.course_key = courses.course_key
    join {{ source('event_sink', 'course_block_names')}} blocks
         on transcripts.video_id = blocks.location
