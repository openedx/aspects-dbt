with
    responses as (
        select
            emission_time,
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            responses,
            success,
            attempts,
            interaction_type
        from {{ ref("problem_events") }}
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
    {{ a_tag("responses.object_id", "blocks.block_name") }} as problem_link,
    blocks.section_block_id as section_block_id,
    blocks.subsection_block_id as subsection_block_id,
    blocks.graded as graded,
    course_order as course_order,
    responses.actor_id as actor_id,
    responses.responses as responses,
    responses.success as success,
    responses.attempts as attempts,
    responses.interaction_type as interaction_type
from responses
join
    {{ ref("dim_course_blocks") }} blocks
    on responses.problem_id = blocks.block_id
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
    section_block_id,
    subsection_block_id,
    actor_id,
    responses,
    success,
    attempts,
    course_order,
    graded,
    interaction_type
