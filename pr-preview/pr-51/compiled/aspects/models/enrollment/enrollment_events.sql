

select
    event_id,
    cast(emission_time as DateTime) as emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] as course_key,
    org,
    verb_id,
    JSON_VALUE(
        event,
        '$.object.definition.extensions."https://w3id.org/xapi/acrossx/extensions/type"'
    ) as enrollment_mode
from `xapi`.`xapi_events_all_parsed`
where
    verb_id in (
        'http://adlnet.gov/expapi/verbs/registered',
        'http://id.tincanapi.com/verb/unregistered'
    )