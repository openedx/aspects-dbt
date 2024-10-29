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
    {{ get_bucket("course_grade") }} as grade_bucket,
    users.username as username,
    users.name as name,
    users.email as email,
    fes.emission_time as enrolled_at
from {{ ref("fact_enrollment_status") }} fes FINAL
left join
    {{ ref("fact_learner_course_status") }} lg
    on fes.org = lg.org
    and fes.course_key = lg.course_key
    and fes.actor_id = lg.actor_id
left join
    {{ ref("fact_learner_course_grade") }} ls
    on fes.org = ls.org
    and fes.course_key = ls.course_key
    and fes.actor_id = ls.actor_id
join
    {{ ref("course_names") }} courses
    on fes.org = courses.org
    and fes.course_key = courses.course_key
left outer join
    {{ ref("dim_user_pii") }} users
    on (actor_id like 'mailto:%' and SUBSTRING(actor_id, 8) = users.email)
    or actor_id = toString(users.external_user_id)
