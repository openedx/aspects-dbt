
  
    
  
    
    
    
        
         


        insert into `xapi`.`fact_instance_enrollments`
        ("emission_day", "course_key", "enrollment_mode", "enrollment_status", "course_enrollment_mode_status_cnt")

with
    enrollments as (
        select
            emission_time,
            course_key,
            enrollment_mode,
            splitByString('/', verb_id)[-1] as enrollment_status
        from `xapi`.`enrollment_events`
    )

select
    date_trunc('day', emission_time) as emission_day,
    enrollments.course_key,
    enrollments.enrollment_mode as enrollment_mode,
    enrollments.enrollment_status as enrollment_status,
    count() as course_enrollment_mode_status_cnt
from enrollments
group by emission_day, course_key, enrollment_mode, enrollment_status
  
  
  