-- problem_results should only have one record for the following
-- combination of values:
-- actor_id, problem_id, course_id, org
select org, course_key, problem_id, actor_id, responses, count(*) as num_rows
from {{ ref("int_problem_results") }}
group by org, course_key, problem_id, actor_id, responses
having num_rows > 1
