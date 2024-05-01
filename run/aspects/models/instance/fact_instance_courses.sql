
  
    
    
    
        
        insert into `xapi`.`fact_instance_courses__dbt_backup`
        ("emission_hour", "courses_cnt")

select
    date_trunc('hour', emission_time) as emission_hour,
    uniqCombinedState(course_id) as courses_cnt
from `xapi`.`xapi_events_all_parsed`
group by emission_hour
  
  