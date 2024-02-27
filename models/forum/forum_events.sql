{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, verb_id)",
        order_by="(org, course_key, verb_id, emission_time, actor_id, object_id, event_id)",
        partition_by="(toYYYYMM(emission_time))",
    )
}}

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    org,
    splitByString('/', course_id)[-1] as course_key,
    object_id,
    actor_id,
    verb_id
from {{ ref("xapi_events_all_parsed") }}
where
    JSON_VALUE(event, '$.object.definition.type')
    = 'http://id.tincanapi.com/activitytype/discussion'
