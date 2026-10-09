-- The post_hook fixes tables created when emission_time_long was stored as DateTime.
{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, verb_id)",
        order_by="(org, course_key, verb_id, emission_time, actor_id, video_position, event_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
        post_hook=[
            "alter table {{ this }} {{ on_cluster() }} modify column emission_time_long DateTime64(6)",
        ],
    )
}}

with
    final_results as (
        select
            event_id,
            emission_time,
            -- In the final select, emission_time would resolve to its DateTime alias there.
            toDateTime64(emission_time, 6) as _emission_time_long,
            actor_id,
            object_id,
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
                        '0.0'
                    ),
                    'Decimal32(2)'
                )
            ) as _video_position,
            JSONExtractInt(
                event,
                'context',
                'extensions',
                'https://w3id.org/xapi/video/extensions/length'
            ) as video_duration
        from {{ ref("xapi_events_all_parsed") }}
        where
            (
                verb_id in (
                    'http://adlnet.gov/expapi/verbs/completed',
                    'http://adlnet.gov/expapi/verbs/initialized',
                    'http://adlnet.gov/expapi/verbs/terminated',
                    'https://w3id.org/xapi/video/verbs/paused',
                    'https://w3id.org/xapi/video/verbs/played',
                    'https://w3id.org/xapi/video/verbs/seeked'
                )
            )
            and (object_id like '%video+block%')
    )
select
    event_id,
    _emission_time_long as emission_time_long,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    course_key,
    org,
    verb_id,
    -- reset video position when it has been re-played from the end
    case
        when
            _video_position = video_duration
            and verb_id = 'https://w3id.org/xapi/video/verbs/played'
        then 1
        else _video_position
    end as video_position,
    video_duration
from final_results
