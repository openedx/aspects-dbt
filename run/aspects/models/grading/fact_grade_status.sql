

  create view `xapi`.`fact_grade_status` 
  
    
    
  as (
    select
    ls.org as org,
    ls.course_key as course_key,
    ls.actor_id as actor_id,
    courses.course_name as course_name,
    courses.course_run as course_run,
    coalesce(state, 'failed') as state,
    course_grade as course_grade,
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
    end as grade_bucket
from `xapi`.`fact_learner_course_grade` ls
left join
    `xapi`.`fact_learner_course_status` lg
    on ls.org = lg.org
    and ls.course_key = lg.course_key
    and ls.actor_id = lg.actor_id
join
    `event_sink`.`course_names` courses
    on ls.course_key = courses.course_key
  )