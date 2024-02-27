{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, verb_id)",
        order_by="(org, course_key, verb_id, emission_time, actor_id, object_id, scaled_score, event_id)",
        partition_by="(toYYYYMM(emission_time))",
    )
}}


select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] as course_key,
    org,
    verb_id,
    coalesce(JSONExtractFloat(event, 'result', 'score', 'scaled'), 0) as scaled_score
from {{ ref("xapi_events_all_parsed") }}
where
    verb_id in (
        'http://id.tincanapi.com/verb/earned',
        'https://w3id.org/xapi/acrossx/verbs/evaluated',
        'http://adlnet.gov/expapi/verbs/passed',
        'http://adlnet.gov/expapi/verbs/failed'
    )
