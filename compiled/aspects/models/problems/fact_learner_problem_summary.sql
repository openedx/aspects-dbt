-- summary table for a learner's performance on and interactions with a
-- particular problem
with results_with_hints as (
    select
        org,
        course_key,
        course_name,
        course_run,
        problem_id,
        problem_name,
        actor_id,
        success,
        attempts,
        0 as num_hints_displayed,
        0 as num_answers_displayed
    from xapi.int_problem_results
    union all
    select
        org,
        course_key,
        course_name,
        course_run,
        problem_id,
        problem_name,
        actor_id,
        null as success,
        null as attempts,
        case help_type
            when 'hint' then 1
            else 0
        end as num_hints_displayed,
        case help_type
            when 'answer' then 1
            else 0
        end as num_answers_displayed
    from xapi.int_problem_hints
)

-- n.b.: there should only be one row per org, course, problem, and actor
-- in problem_results, so any(success) and any(attempts) should return the
-- values from that part of the union and not the null values used as
-- placeholders in the problem_hints part of the union
select
    org,
    course_key,
    course_name,
    course_run,
    problem_id,
    problem_name,
    actor_id,
    coalesce(any(success), false) as success,
    coalesce(any(attempts), 0) as attempts,
    sum(num_hints_displayed) as num_hints_displayed,
    sum(num_answers_displayed) as num_answers_displayed
from
    results_with_hints
group by
    org,
    course_key,
    course_name,
    course_run,
    problem_id,
    problem_name,
    actor_id