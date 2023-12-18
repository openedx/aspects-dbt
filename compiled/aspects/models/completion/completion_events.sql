

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] AS course_key,
    org,
    verb_id,
    JSON_VALUE(event_str, '$.result.extensions."https://w3id.org/xapi/cmi5/result/extensions/progress"') AS progress_percent
FROM `xapi`.`xapi_events_all_parsed`
WHERE verb_id = 'http://adlnet.gov/expapi/verbs/progressed'