select
    fes.org as org,
    fes.course_key as course_key,
    fes.course_run as course_run,
    fes.actor_id as actor_id,
    courses.course_name as course_name,
    if(empty(approving_state), 'failed', approving_state) as approving_state,
    enrollment_mode,
    enrollment_status,
    course_grade as course_grade,
    {{ get_bucket("course_grade") }} as grade_bucket
from {{ ref("fact_enrollment_status") }} fes
left join
    {{ ref("fact_learner_course_status") }} lg
    on fes.org = lg.org
    and fes.course_key = lg.course_key
    and fes.course_run = lg.course_run
    and fes.actor_id = lg.actor_id
left join
    {{ ref("fact_learner_course_grade") }} ls
    on fes.org = fes.org
    and fes.course_key = ls.course_key
    and fes.course_run = ls.course_run
    and fes.actor_id = ls.actor_id
join
    {{ source("event_sink", "course_names") }} courses
    on fes.org = courses.org
    and fes.course_key = courses.course_key
    and fes.course_run = courses.course_run
