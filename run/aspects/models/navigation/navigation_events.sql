
  
    
    
    
        
        insert into `xapi`.`navigation_events__dbt_backup`
        ("event_id", "emission_time", "actor_id", "block_id", "course_key", "org", "verb_id", "object_type", "starting_position", "ending_point")

select
    event_id,
    cast(emission_time as DateTime) as emission_time,
    actor_id,
    splitByString('/xblock/', object_id)[-1] as block_id,
    splitByString('/', course_id)[-1] as course_key,
    org,
    verb_id,
    JSONExtractString(event, 'object', 'definition', 'type') as object_type,
    -- clicking a link and selecting a module outline have no starting-position field
    if(
        object_type in (
            'http://adlnet.gov/expapi/activities/link',
            'http://adlnet.gov/expapi/activities/module'
        ),
        0,
        JSONExtractInt(
            event,
            'context',
            'extensions',
            'http://id.tincanapi.com/extension/starting-position'
        )
    ) as starting_position,
    JSONExtractString(
        event, 'context', 'extensions', 'http://id.tincanapi.com/extension/ending-point'
    ) as ending_point
from `xapi`.`xapi_events_all_parsed`
where verb_id in ('https://w3id.org/xapi/dod-isd/verbs/navigated')
  
  