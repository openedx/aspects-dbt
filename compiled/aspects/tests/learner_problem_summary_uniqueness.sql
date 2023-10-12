select
    org,
    course_key,
    problem_id,
    actor_id,
    count(*) as num_rows
from
    reporting.fact_learner_problem_summary
group by
    org,
    course_key,
    problem_id,
    actor_id
having num_rows > 1