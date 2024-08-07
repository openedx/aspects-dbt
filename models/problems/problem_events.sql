{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, verb_id)",
        order_by="(org, course_key, verb_id, emission_time, actor_id, object_id, responses, success, event_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    event_id,
    cast(emission_time as DateTime) as emission_time,
    actor_id,
    object_id,
    course_key,
    org,
    verb_id,
    JSON_VALUE(event, '$.result.response') as responses,
    JSONExtractFloat(event, 'result', 'score', 'scaled') as scaled_score,
    if(
        verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated',
        cast(JSON_VALUE(event, '$.result.success') as Bool),
        false
    ) as success,
    toLowCardinality(
        JSON_VALUE(event, '$.object.definition.interactionType')
    ) as interaction_type,
    if(
        verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated',
        cast(
            JSON_VALUE(
                event,
                '$.object.definition.extensions."http://id.tincanapi.com/extension/attempt-id"'
            ) as Int16
        ),
        0
    ) as attempts,
    {{ get_problem_id("object_id") }} as problem_id
from {{ ref("xapi_events_all_parsed") }}
where
    verb_id in (
        'https://w3id.org/xapi/acrossx/verbs/evaluated',
        'http://adlnet.gov/expapi/verbs/passed',
        'http://adlnet.gov/expapi/verbs/asked'
    )
