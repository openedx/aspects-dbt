
  
    
    
    
        
        insert into `xapi`.`fact_instance_actors__dbt_backup`
        ("emission_day", "actors_cnt")

select
    date_trunc('day', emission_time) as emission_day,
    uniqCombinedState(actor_id) as actors_cnt
from `xapi`.`xapi_events_all_parsed`
group by emission_day
  
  