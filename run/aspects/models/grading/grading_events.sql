
  
    
    
    
        
        insert into `xapi`.`grading_events__dbt_backup`
        ("event_id", "emission_time", "actor_id", "object_id", "course_key", "org", "verb_id", "scaled_score")


select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    actor_id,
    object_id,
    course_key,
    org,
    verb_id,
    JSONExtractFloat(event, 'result', 'score', 'scaled') as scaled_score
from `xapi`.`xapi_events_all_parsed`
where
    verb_id in (
        'http://id.tincanapi.com/verb/earned',
        'https://w3id.org/xapi/acrossx/verbs/evaluated'
    )
    or (
        verb_id in (
            'http://adlnet.gov/expapi/verbs/passed',
            'http://adlnet.gov/expapi/verbs/failed'
        )
        and JSON_VALUE(event::String, '$.object.definition.type')
        = 'http://adlnet.gov/expapi/activities/course'
    )
  
  