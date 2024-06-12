{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(video_id, emission_time, actor_id, cc_enabled)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    org,
    course_key,
    splitByString('/xblock/', object_id)[2] as video_id,
    actor_id,
    JSONExtractBool(
        event,
        'result',
        'extensions',
        'https://w3id.org/xapi/video/extensions/cc-enabled'
    ) as cc_enabled
from {{ ref("xapi_events_all_parsed") }}
where
    verb_id = 'http://adlnet.gov/expapi/verbs/interacted'
    and JSONHas(
        event,
        'result',
        'extensions',
        'https://w3id.org/xapi/video/extensions/cc-enabled'
    )
