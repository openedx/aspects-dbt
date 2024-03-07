
  
    
    
    
        
        insert into `xapi`.`video_playback_events__dbt_backup`
        ("event_id", "emission_time", "actor_id", "object_id", "course_key", "org", "verb_id", "video_position", "video_duration")

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
    ) as video_position,
    JSONExtractInt(
        event, 'context', 'extensions', 'https://w3id.org/xapi/video/extensions/length'
    ) as video_duration
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
  
  