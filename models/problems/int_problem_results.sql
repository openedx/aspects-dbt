-- select one record per (learner, problem, course, org) tuple
-- contains either the first successful attempt
-- or the most recent unsuccessful attempt
-- find the timestamp of the earliest successful response
-- this will be used to pick the xAPI event corresponding to that submission
with
    successful_responses as (
        select
            org,
            course_key,
            problem_id,
            actor_id,
            min(emission_time) as first_success_at
        from {{ ref("problem_responses") }}
        where
            -- clickhouse throws an error when shortening this to `where success`
            success = true
        group by org, course_key, problem_id, actor_id
    ),
    -- for all learners who did not submit a successful response,
    -- find the timestamp of the most recent unsuccessful response
    unsuccessful_responses as (
        select
            org,
            course_key,
            problem_id,
            actor_id,
            max(emission_time) as last_response_at
        from {{ ref("problem_responses") }}
        where actor_id not in (select distinct actor_id from successful_responses)
        group by org, course_key, problem_id, actor_id
    ),
    -- combine result sets for successful and unsuccessful problem submissions
    responses as (
        select org, course_key, problem_id, actor_id, first_success_at as emission_time
        from successful_responses
        union all
        select org, course_key, problem_id, actor_id, last_response_at as emission_time
        from unsuccessful_responses
    )

select
    emission_time,
    org,
    course_key,
    course_name,
    course_run,
    problem_id,
    problem_name,
    problem_name_with_location,
    course_order,
    problem_link,
    actor_id,
    responses,
    success,
    attempts,
    graded,
    interaction_type
from {{ ref("fact_problem_responses") }} problem_responses
join responses using (org, course_key, problem_id, actor_id, emission_time)
