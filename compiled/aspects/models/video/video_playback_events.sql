

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] as course_key,
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
    ) as video_position
from `xapi`.`xapi_events_all_parsed`
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