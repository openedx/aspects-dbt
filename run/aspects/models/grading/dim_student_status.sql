

  create or replace view `xapi`.`dim_student_status` 
  
    
  
  
    
    
  as (
    select
    most_recent_enrollment.org as org,
    most_recent_enrollment.course_key as course_key,
    most_recent_enrollment.actor_id as actor_id,
    courses.course_name as course_name,
    courses.course_run as course_run,
    if(
        empty(course_state.approving_state), 'failed', course_state.approving_state
    ) as approving_state,
    most_recent_enrollment.enrollment_mode as enrollment_mode,
    most_recent_enrollment.enrollment_status as enrollment_status,
    course_grade.course_grade as course_grade,
    case
        when course_grade.course_grade >= 0.9
        then '90-100%'
        when course_grade.course_grade >= 0.8
        then '80-89%'
        when course_grade.course_grade >= 0.7
        then '70-79%'
        when course_grade.course_grade >= 0.6
        then '60-69%'
        when course_grade.course_grade >= 0.5
        then '50-59%'
        when course_grade.course_grade >= 0.4
        then '40-49%'
        when course_grade.course_grade >= 0.3
        then '30-39%'
        when course_grade.course_grade >= 0.2
        then '20-29%'
        when course_grade.course_grade >= 0.1
        then '10-19%'
        else '0-9%'
    end as grade_bucket,
    users.username as username,
    users.name as name,
    users.email as email,
    most_recent_enrollment.emission_time as enrolled_at
from `xapi`.`dim_most_recent_enrollment` most_recent_enrollment
left join
    `xapi`.`dim_learner_most_recent_course_state` course_state
    on most_recent_enrollment.org = course_state.org
    and most_recent_enrollment.course_key = course_state.course_key
    and most_recent_enrollment.actor_id = course_state.actor_id
left join
    `xapi`.`dim_learner_most_recent_course_grade` course_grade
    on most_recent_enrollment.org = course_grade.org
    and most_recent_enrollment.course_key = course_grade.course_key
    and most_recent_enrollment.actor_id = course_grade.actor_id
join
    `xapi`.`dim_course_names` courses
    on most_recent_enrollment.org = courses.org
    and most_recent_enrollment.course_key = courses.course_key
left outer join
    `xapi`.`dim_user_pii` users
    on (
        most_recent_enrollment.actor_id like 'mailto:%'
        and SUBSTRING(most_recent_enrollment.actor_id, 8) = users.email
    )
    or most_recent_enrollment.actor_id = toString(users.external_user_id)
    
  )
      
      
                    -- end_of_sql
                    
                    