
  
    
    
    
        
        insert into `xapi`.`fact_learner_last_course_visit__dbt_backup`
        ("org", "course_key", "actor_id", "emission_time")

select org, course_key, actor_id, max(emission_time) as emission_time
from `xapi`.`navigation_events`
group by org, course_key, actor_id
  
  