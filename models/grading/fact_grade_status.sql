select
    ls.org as org,
    ls.course_key as course_key,
    ls.actor_id as actor_id,
    state as state,
    course_grade as course_grade
from {{ ref("fact_learner_course_grade") }} ls
left join
    {{ ref("fact_learner_course_status") }} lg
    on ls.org = lg.org
    and ls.course_key = lg.course_key
    and ls.actor_id = lg.actor_id
