

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    course_key,
    org,
    verb_id,
    JSON_VALUE(
        event,
        '$.result.extensions."https://w3id.org/xapi/cmi5/result/extensions/progress"'
    ) as progress_percent
from `xapi`.`xapi_events_all_parsed`
where verb_id = 'http://adlnet.gov/expapi/verbs/progressed'