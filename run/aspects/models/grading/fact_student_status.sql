

  create view `xapi`.`fact_student_status` 
  
    
    
  as (
    select
    fes.org as org,
    fes.course_key as course_key,
    fes.actor_id as actor_id,
    courses.course_name as course_name,
    courses.course_run as course_run,
    if(empty(approving_state), 'failed', approving_state) as approving_state,
    enrollment_mode,
    enrollment_status,
    course_grade,
    case
        when course_grade >= 0.9
        then '90-100%'
        when course_grade >= 0.8 and course_grade < 0.9
        then '80-89%'
        when course_grade >= 0.7 and course_grade < 0.8
        then '70-79%'
        when course_grade >= 0.6 and course_grade < 0.7
        then '60-69%'
        when course_grade >= 0.5 and course_grade < 0.6
        then '50-59%'
        when course_grade >= 0.4 and course_grade < 0.5
        then '40-49%'
        when course_grade >= 0.3 and course_grade < 0.4
        then '30-39%'
        when course_grade >= 0.2 and course_grade < 0.3
        then '20-29%'
        when course_grade >= 0.1 and course_grade < 0.2
        then '10-19%'
        else '0-9%'
    end as grade_bucket,
    users.username as username,
    users.name as name,
    users.email as email,
    fes.emission_time as enrolled_at
from `xapi`.`fact_enrollment_status` fes
left join
    `xapi`.`fact_learner_course_status` lg
    on fes.org = lg.org
    and fes.course_key = lg.course_key
    and fes.actor_id = lg.actor_id
left join
    `xapi`.`fact_learner_course_grade` ls
    on fes.org = ls.org
    and fes.course_key = ls.course_key
    and fes.actor_id = ls.actor_id
join
    `xapi`.`course_names` courses
    on fes.org = courses.org
    and fes.course_key = courses.course_key
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
  )
      
      
                    -- end_of_sql
                    
                    