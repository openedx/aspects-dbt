
  
    
    
    
        
         


        insert into `xapi`.`fact_instance_events__dbt_backup`
        ("emission_day", "events_cnt")

select
    date_trunc('day', emission_time) as emission_day,
    uniqCombinedState(event_id) as events_cnt
from `xapi`.`xapi_events_all_parsed`
group by emission_day
  