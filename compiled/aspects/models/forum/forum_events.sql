

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    org,
    splitByString('/', course_id)[-1] AS course_key,
    object_id,
    actor_id,
    verb_id
FROM `xapi`.`xapi_events_all_parsed`
WHERE JSON_VALUE(event, '$.object.definition.type') = 'http://id.tincanapi.com/activitytype/discussion'