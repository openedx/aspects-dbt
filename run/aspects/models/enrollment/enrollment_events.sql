
  
    
    
    
        
         


        insert into `xapi`.`enrollment_events__dbt_backup`
        ("event_id", "emission_time", "actor_id", "object_id", "course_key", "org", "verb_id", "enrollment_mode", "enrollment_status")

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
from `xapi`.`xapi_events_all_parsed`
where
    verb_id in (
        'http://adlnet.gov/expapi/verbs/registered',
        'http://id.tincanapi.com/verb/unregistered'
    )
  