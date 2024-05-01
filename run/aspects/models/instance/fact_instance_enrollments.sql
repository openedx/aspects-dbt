
  
    
    
    
        
        insert into `xapi`.`fact_instance_enrollments__dbt_backup`
        ("emission_hour", "course_name", "enrollment_mode", "enrollment_status", "course_enrollment_mode_status_cnt")

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
    date_trunc('hour', emission_time) as emission_hour,
    courses.course_name as course_name,
    enrollments.enrollment_mode as enrollment_mode,
    enrollments.enrollment_status as enrollment_status,
    count() as course_enrollment_mode_status_cnt
from enrollments
join `xapi`.`course_names` courses on enrollments.course_key = courses.course_key
group by emission_hour, course_name, enrollment_mode, enrollment_status
  
  