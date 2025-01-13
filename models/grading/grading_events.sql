{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, verb_id)",
        order_by="(org, course_key, verb_id, emission_time, actor_id, object_id, scaled_score, event_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}


select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    course_key,
    org,
    verb_id,
    JSONExtractFloat(event, 'result', 'score', 'scaled') as scaled_score,
    CASE 
      WHEN JSONExtractString(event, 'result', 'extensions', 'http://www.tincanapi.co.uk/activitytypes/grade_classification') = 'Fail' THEN 'failed'
      WHEN JSONExtractString(event, 'result', 'extensions', 'http://www.tincanapi.co.uk/activitytypes/grade_classification') = 'Pass' THEN 'passed' 
      WHEN verb_id IN (
        'http://adlnet.gov/expapi/verbs/passed',
        'http://adlnet.gov/expapi/verbs/failed'
    ) THEN splitByString('/', verb_id)[-1]
    ELSE ''
    END AS approving_state
from {{ ref("xapi_events_all_parsed") }}
where
    verb_id in (
        'http://id.tincanapi.com/verb/earned',
        'https://w3id.org/xapi/acrossx/verbs/evaluated'
    )
    or (
        verb_id in (
            'http://adlnet.gov/expapi/verbs/passed',
            'http://adlnet.gov/expapi/verbs/failed'
        )
        and JSON_VALUE(event::String, '$.object.definition.type')
        = 'http://adlnet.gov/expapi/activities/course'
    )
