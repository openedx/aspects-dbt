with
    responses as (
        select
            emission_time,
            org,
            course_key,
            object_id,
            
    regexpExtract(
        object_id, 'xblock/([\w\d-\+:@]*@problem\+block@[\w\d][^_]*)(_\d_\d)?', 1
    )
 as problem_id,
            actor_id,
            responses,
            success,
            attempts,
            interaction_type
        from `xapi`.`problem_events`
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    )

select
    responses.emission_time as emission_time,
    responses.org as org,
    responses.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    responses.problem_id as problem_id,
    blocks.block_name as problem_name,
    blocks.display_name_with_location as problem_name_with_location,
    
    concat(
        '<a href="', responses.object_id, '" target="_blank">', blocks.block_name, '</a>'
    )
 as problem_link,
    blocks.graded as graded,
    responses.actor_id as actor_id,
    responses.responses as responses,
    responses.success as success,
    responses.attempts as attempts,
    responses.interaction_type as interaction_type,
    users.username as username,
    users.name as name,
    users.email as email
from responses
join
    `xapi`.`dim_course_blocks` blocks
    on (
        responses.course_key = blocks.course_key
        and responses.problem_id = blocks.block_id
    )
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
group by
    -- multi-part questions include an extra record for the response to the first
    -- part of the question. this group by clause eliminates the duplicate record
    emission_time,
    org,
    course_key,
    course_name,
    course_run,
    problem_id,
    problem_name,
    problem_name_with_location,
    problem_link,
    actor_id,
    responses,
    success,
    attempts,
    graded,
    interaction_type,
    username,
    name,
    email