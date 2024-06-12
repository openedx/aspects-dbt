{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, video_id)",
        order_by="(org, course_key, video_id, emission_time, actor_id, video_position)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    splitByString('/xblock/', object_id)[-1] as video_id,
    course_key,
    org,
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
where verb_id = 'https://w3id.org/xapi/video/verbs/played'
