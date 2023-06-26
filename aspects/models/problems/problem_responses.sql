-- this model could support the answer distribution and problem
-- submission count metrics
select
    emission_time,
    org,
    course_id,
    {{ get_problem_id('object_id') }} as problem_id,
    actor_id,
    responses,
    success,
    attempts
from
    {{ source('xapi', 'problem_events') }}
where
    verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
