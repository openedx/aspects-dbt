

SELECT
    event_id as event_id,
    JSON_VALUE(event_str, '$.verb.id') as verb_id,
    COALESCE(
        NULLIF(JSON_VALUE(event_str, '$.actor.account.name'), ''),
        NULLIF(JSON_VALUE(event_str, '$.actor.mbox'), ''),
        JSON_VALUE(event_str, '$.actor.mbox_sha1sum')
    ) as actor_id,
    JSON_VALUE(event_str, '$.object.id') as object_id,
    -- If the contextActivities parent is a course, use that. Otherwise use the object id for the course id
    if(
        JSON_VALUE(
            event_str,
            '$.context.contextActivities.parent[0].definition.type')
                = 'http://adlnet.gov/expapi/activities/course',
            JSON_VALUE(event_str, '$.context.contextActivities.parent[0].id'),
            JSON_VALUE(event_str, '$.object.id')
        ) as course_id,
    coalesce(get_org_from_course_url(course_id), '') as org,
    emission_time as emission_time,
    event_str as event_str
FROM `xapi`.`xapi_events_all`