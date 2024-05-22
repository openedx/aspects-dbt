create materialized view if not exists `xapi`.`fact_learner_last_course_visit_mv` 
  
  to `xapi`.`fact_learner_last_course_visit`
  as 

select org, course_key, actor_id, max(emission_time) as emission_time
from `xapi`.`navigation_events`
group by org, course_key, actor_id