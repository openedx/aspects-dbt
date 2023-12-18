

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] AS course_key,
    org,
    verb_id,
    ceil(CAST(coalesce(nullIf(JSON_VALUE(event_str, '$.result.extensions."https://w3id.org/xapi/video/extensions/time"'), ''), nullIf(JSON_VALUE(event_str, '$.result.extensions."https://w3id.org/xapi/video/extensions/time-from"'), ''), '0.0'), 'Decimal32(2)')) AS video_position
FROM `xapi`.`xapi_events_all_parsed`
WHERE (verb_id IN ('http://adlnet.gov/expapi/verbs/completed', 'http://adlnet.gov/expapi/verbs/initialized', 'http://adlnet.gov/expapi/verbs/terminated', 'https://w3id.org/xapi/video/verbs/paused', 'https://w3id.org/xapi/video/verbs/played', 'https://w3id.org/xapi/video/verbs/seeked')) AND (object_id LIKE '%video+block%')