{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key)",
        order_by="(org, course_key, emission_time, actor_id, enrollment_mode, enrollment_status, event_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
        full_refresh=true,
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
    toLowCardinality(
        JSON_VALUE(
            event,
            '$.object.definition.extensions."https://w3id.org/xapi/acrossx/extensions/type"'
        )
    ) as enrollment_mode,
    splitByString('/', verb_id)[-1] as enrollment_status
from {{ ref("xapi_events_all_parsed") }}
where
    verb_id in (
        'http://adlnet.gov/expapi/verbs/registered',
        'http://id.tincanapi.com/verb/unregistered'
    )
