

  create view `xapi`.`dim_at_risk_learners` 
  
    
    
  as (
    with
    page_visits as (
        select org, course_key, actor_id, max(emission_time) as last_visited
        from `xapi`.`navigation_events`
        group by org, course_key, actor_id
    )

select
    learners.org as org,
    learners.course_key as course_key,
    learners.course_name as course_name,
    learners.course_run as course_run,
    learners.actor_id as actor_id,
    learners.username as username,
    learners.name as name,
    learners.email as email,
    learners.enrollment_mode as enrollment_mode,
    learners.course_grade as course_grade,
    learners.enrolled_at as enrolled_at,
    learners.grade_bucket as grade_bucket,
    page_visits.last_visited as last_visited
from `xapi`.`fact_student_status` learners
join page_visits using (org, course_key, actor_id)
where
    approving_state = 'failed'
    and enrollment_status = 'registered'
    and page_visits.last_visited < subtractDays(now(), 7)
  )
      
      
                    -- end_of_sql
                    
                    