-- select one record per (learner, problem, course, org) tuple
-- contains either the first successful attempt
-- or the most recent unsuccessful attempt

with outcomes as (
    select emission_time,
           org,
           course_key,
           problem_id,
           actor_id,
           success,
           first_value(success) over (partition by course_key,
                                                   problem_id,
                                                   actor_id
                                      order by success desc) as was_successful
    from problem_responses
),
responses as (
    select org, course_key, problem_id, actor_id,
    if(
        was_successful,
        minIf(emission_time, was_successful),
        maxIf(emission_time, not was_successful)
    ) as emission_time
      from outcomes
      group by org, course_key, problem_id, actor_id, was_successful
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
    actor_id,
    responses,
    success,
    attempts
from
    {{ ref('fact_problem_responses') }} problem_responses
    join responses
        using (org, course_key, problem_id, actor_id, emission_time)
