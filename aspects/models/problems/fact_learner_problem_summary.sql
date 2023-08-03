-- summary table for a learner's performance on and interactions with a
-- particular problem
with results_with_hints as (
    select
        org,
        course_key,
        problem_id,
        actor_id,
        success,
        attempts,
        0 as num_hints_displayed,
        0 as num_answers_displayed
    from {{ ref('int_problem_results') }}
    union all
    select
        org,
        course_key,
        problem_id,
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
    from {{ ref('int_problem_hints') }}
), summary as (
    -- n.b.: there should only be one row per org, course, problem, and actor
    -- in problem_results, so any(success) and any(attempts) should return the
    -- values from that part of the union and not the null values used as
    -- placeholders in the problem_hints part of the union
    select
        org,
        course_key,
        problem_id,
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
        problem_id,
        actor_id
)

select
    summary.org as org,
    courses.course_name as course_name,
    splitByString('+', courses.course_key)[-1] as run_name,
    blocks.block_name as problem_name,
    summary.actor_id as actor_id,
    summary.success as success,
    summary.attempts as attempts,
    summary.num_hints_displayed as num_hints_displayed,
    summary.num_answers_displayed as num_answers_displayed
from
    summary
    join {{ source('event_sink', 'course_names')}} courses
         on summary.course_key = courses.course_key
    join {{ source('event_sink', 'course_block_names')}} blocks
         on summary.problem_id = blocks.location
