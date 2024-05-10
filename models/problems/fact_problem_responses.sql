with
    responses as (
        select *
        from {{ ref("problem_responses") }}
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
    {{ a_tag("responses.object_id", "blocks.block_name") }} as problem_link,
    blocks.graded as graded,
    course_order as course_order,
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
    {{ ref("dim_course_blocks") }} blocks
    on (
        responses.course_key = blocks.course_key
        and responses.problem_id = blocks.block_id
    )
left outer join
    {{ ref("dim_user_pii") }} users on toUUID(actor_id) = users.external_user_id
