{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(course_key, block_id_short, actor_id)",
        order_by="(course_key, block_id_short, actor_id)",
    )
}}

with
    final_results as (
        select
            last_response.org as org,
            last_response.course_key as course_key,
            last_response.success as success,
            last_response.attempts as attempts,
            last_response.actor_id as actor_id,
            splitByChar('@', last_response.problem_id)[3] as block_id_short,
            {{
                format_problem_number_location(
                    "last_response.object_id", "blocks.display_name_with_location"
                )
            }}
        from {{ ref("dim_learner_last_response") }} last_response
        left join
            {{ ref("dim_course_blocks") }} blocks
            on (
                last_response.course_key = blocks.course_key
                and last_response.problem_id = blocks.block_id
            )
    )
select
    org,
    course_key,
    success,
    attempts,
    actor_id,
    problem_number,
    problem_name_location,
    block_id_short
from final_results
