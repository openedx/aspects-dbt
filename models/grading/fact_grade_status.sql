select
    ls.org as org,
    ls.course_key as course_key,
    ls.actor_id as actor_id,
    courses.course_name as course_name,
    courses.course_run as course_run,
    coalesce(state, 'failed') as state,
    course_grade as course_grade,
    {{ get_bucket("course_grade") }} as grade_bucket
from {{ ref("fact_learner_course_grade") }} ls
left join
    {{ ref("fact_learner_course_status") }} lg
    on ls.org = lg.org
    and ls.course_key = lg.course_key
    and ls.actor_id = lg.actor_id
join
    {{ source("event_sink", "course_names") }} courses
    on ls.course_key = courses.course_key
