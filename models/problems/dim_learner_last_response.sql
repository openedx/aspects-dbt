{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, problem_id, actor_id)",
        primary_key="(org, course_key, problem_id, actor_id)",
    )
}}

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
        from {{ ref("problem_events") }} events
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
        group by org, course_key, object_id, problem_id, actor_id, interaction_type
    )
select
    final_results.org as org,
    final_results.course_key as course_key,
    final_results.object_id as object_id,
    final_results.problem_id as problem_id,
    concat(
        '<a href="',
        final_results.object_id,
        '" target="_blank">',
        blocks.display_name_with_location,
        '</a>'
    ) as problem_link,
    blocks.display_name_with_location as display_name_with_location,
    blocks.graded as graded,
    final_results.actor_id as actor_id,
    blocks.course_order as course_order,
    final_results.interaction_type as interaction_type,
    final_results._attempt as attempts,
    final_results.success as success,
    final_results.emission_time as emission_time,
    final_results.responses as responses,
    final_results.scaled_score as scaled_score
from final_results
left join
    {{ ref("dim_course_blocks") }} blocks
    on (
        final_results.course_key = blocks.course_key
        and final_results.problem_id = blocks.block_id
    )
