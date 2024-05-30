

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    org,
    course_key,
    object_id,
    actor_id,
    verb_id
from `xapi`.`xapi_events_all_parsed`
where
    JSON_VALUE(event, '$.object.definition.type')
    = 'http://id.tincanapi.com/activitytype/discussion'