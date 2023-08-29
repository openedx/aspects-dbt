with responses as (
    select
        emission_time,
        org,
        course_key,
        {{ get_problem_id('object_id') }} as problem_id,
        actor_id,
        responses,
        success,
        attempts
    from
        {{ source('xapi', 'problem_events') }}
    where
        verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
)

select
    responses.emission_time as emission_time,
    responses.org as org,
    responses.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    responses.problem_id as problem_id,
    blocks.block_name as problem_name,
    responses.actor_id as actor_id,
    responses.responses as responses,
    responses.success as success,
    responses.attempts as attempts
from
    responses
    join {{ source('event_sink', 'course_names')}} courses
         on responses.course_key = courses.course_key
    join {{ source('event_sink', 'course_block_names')}} blocks
         on responses.problem_id = blocks.location
