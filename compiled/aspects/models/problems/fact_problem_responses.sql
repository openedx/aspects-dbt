with responses as (
    select
        emission_time,
        org,
        course_key,
        
   regexpExtract(object_id, 'xblock/([\w\d-@\+:]*)', 1)
 as problem_id,
        actor_id,
        responses,
        success,
        attempts
    from
        xapi.problem_events
    where
        verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
)

select
    responses.emission_time as emission_time,
    responses.org as org,
    responses.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    responses.problem_id as problem_id,
    blocks.block_name as problem_name,
    responses.actor_id as actor_id,
    responses.responses as responses,
    responses.success as success,
    responses.attempts as attempts
from
    responses
    join reporting.dim_course_blocks blocks
         on (responses.course_key = blocks.course_key
             and responses.problem_id = blocks.block_id)