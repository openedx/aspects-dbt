{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, video_id, verb_id, emission_time, actor_id, video_position)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    org,
    course_key,
    splitByString('/xblock/', object_id)[-1] as video_id,
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    verb_id,
    ceil(
        CAST(
            coalesce(
                nullIf(
                    JSON_VALUE(
                        event,
                        '$.result.extensions."https://w3id.org/xapi/video/extensions/time"'
                    ),
                    ''
                ),
                nullIf(
                    JSON_VALUE(
                        event,
                        '$.result.extensions."https://w3id.org/xapi/video/extensions/time-from"'
                    ),
                    ''
                ),
                '0'
            ),
            'Decimal32(2)'
        )
    ) as video_position,
    JSONExtractInt(
        event, 'context', 'extensions', 'https://w3id.org/xapi/video/extensions/length'
    ) as video_duration
from {{ ref("xapi_events_all_parsed") }}
where
    (
        verb_id in (
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/seeked',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated'
        )
    )
    and (object_id like '%video+block%')
