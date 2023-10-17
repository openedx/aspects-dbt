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
    transcripts.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    transcripts.video_id as video_id,
    blocks.block_name as video_name,
    blocks.display_name_with_location as video_name_with_location,
    transcripts.actor_id as actor_id
from
    transcripts
    join {{ ref('dim_course_blocks')}} blocks
         on (transcripts.course_key = blocks.course_key
             and transcripts.video_id = blocks.block_id)
