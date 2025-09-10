

with
    final_results as (
        select
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            interaction_type,
            MAX(attempts) as _attempt,
            argMax(success, attempts) as success,
            argMax(emission_time, attempts) as emission_time,
            argMax(responses, attempts) as responses,
            argMax(scaled_score, attempts) as scaled_score
        from `xapi`.`problem_events` events
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
        group by org, course_key, object_id, problem_id, actor_id, interaction_type
    )
select
    org,
    course_key,
    object_id,
    problem_id,
    
    concat(
        '<a href="', object_id, '" target="_blank">', blocks.display_name_with_location, '</a>'
    )
 as problem_link,
    blocks.display_name_with_location as display_name_with_location,
    blocks.graded as graded,
    actor_id,
    blocks.course_order as course_order,
    interaction_type,
    _attempt as attempts,
    success,
    emission_time,
    responses,
    scaled_score
from final_results
left join
    `xapi`.`dim_course_blocks` blocks
    on (
        final_results.course_key = blocks.course_key
        and final_results.problem_id = blocks.block_id
    )