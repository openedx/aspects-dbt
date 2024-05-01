
  
    
    
    
        
        insert into `xapi`.`fact_instance_events__dbt_backup`
        ("emission_hour", "events_cnt")

select
    date_trunc('hour', emission_time) as emission_hour,
    uniqCombinedState(event_id) as events_cnt
from `xapi`.`xapi_events_all_parsed`
group by emission_hour
  
  