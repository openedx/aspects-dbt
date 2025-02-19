
  
    
  
    
    
    
        
         


        insert into `xapi`.`fact_enrollment_status`
        ("org", "course_key", "actor_id", "enrollment_status", "enrollment_mode", "emission_time")

with
    ranked_enrollments as (
        select
            emission_time,
            org,
            course_key,
            actor_id,
            enrollment_mode,
            splitByString('/', verb_id)[-1] as enrollment_status,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from `xapi`.`enrollment_events`
    )

select org, course_key, actor_id, enrollment_status, enrollment_mode, emission_time
from ranked_enrollments
where rn = 1
  
  
  