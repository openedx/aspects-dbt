

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    org,
    splitByString('/', course_id)[-1] AS course_key,
    splitByString('/xblock/', object_id)[2] as video_id,
    actor_id,
    JSONExtractBool(event_str, 'result','extensions','https://w3id.org/xapi/video/extensions/cc-enabled') as cc_enabled
FROM `xapi`.`xapi_events_all_parsed`
WHERE
    verb_id IN ('http://adlnet.gov/expapi/verbs/interacted')
    AND JSONHas(event_str, 'result', 'extensions', 'https://w3id.org/xapi/video/extensions/cc-enabled')