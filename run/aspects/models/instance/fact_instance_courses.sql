create materialized view if not exists `xapi`.`fact_instance_courses_mv` 
  
  to `xapi`.`fact_instance_courses`
  as 

select
    date_trunc('hour', emission_time) as emission_hour,
    uniqCombinedState(course_id) as courses_cnt
from `xapi`.`xapi_events_all_parsed`
group by emission_hour