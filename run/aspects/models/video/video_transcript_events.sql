
  
    
    
    
        
        insert into `xapi`.`video_transcript_events__dbt_backup`
        ("event_id", "emission_time", "org", "course_key", "video_id", "actor_id", "cc_enabled")

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
from `xapi`.`xapi_events_all_parsed`
where
    verb_id = 'http://adlnet.gov/expapi/verbs/interacted'
    and JSONHas(
        event,
        'result',
        'extensions',
        'https://w3id.org/xapi/video/extensions/cc-enabled'
    )
  
  