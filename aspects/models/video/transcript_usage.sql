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
