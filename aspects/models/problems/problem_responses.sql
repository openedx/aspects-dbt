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
