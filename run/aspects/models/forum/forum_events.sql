
  
    
    
    
        
        insert into `xapi`.`forum_events__dbt_backup`
        ("event_id", "emission_time", "org", "course_key", "object_id", "actor_id", "verb_id")

select
    event_id,
    CAST(emission_time, 'DateTime') as emission_time,
    org,
    splitByString('/', course_id)[-1] as course_key,
    object_id,
    actor_id,
    verb_id
from `xapi`.`xapi_events_all_parsed`
where
    JSON_VALUE(event, '$.object.definition.type')
    = 'http://id.tincanapi.com/activitytype/discussion'
  
  