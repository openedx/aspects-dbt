

with
    problem_events as (
        select
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            interaction_type,
            argMin(attempts, emission_time) as attempt,
            success,
            argMin(responses, emission_time) as responses,
            argMin(scaled_score, emission_time) as scaled_score,
            MIN(emission_time) as _emission_time
        from `xapi`.`problem_events`
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated' and success
        group by
            org, course_key, object_id, problem_id, actor_id, interaction_type, success
    )
select
    org,
    course_key,
    object_id,
    problem_id,
    actor_id,
    interaction_type,
    attempt,
    success,
    responses,
    scaled_score,
    _emission_time as emission_time
from problem_events