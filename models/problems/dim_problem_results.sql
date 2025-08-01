{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(course_key, block_id_short)",
        order_by="(course_key, block_id_short, actor_id)",
    )
}}

with
    final_results as (
        select
            events.org as org,
            events.course_key as course_key,
            first_success.success as success,
            first_success.attempt as attempt,
            first_success.actor_id as actor_id,
            splitByChar('@', blocks.block_id)[3] as block_id_short,
            {{
                format_problem_number_location(
                    "events.object_id", "blocks.display_name_with_location"
                )
            }}
        from
            (
                select distinct org, course_key, object_id, problem_id
                from {{ ref("problem_events") }} events
            ) events
        join
            {{ ref("dim_course_blocks") }} as blocks
            on (
                events.course_key = blocks.course_key
                and events.problem_id = blocks.block_id
            )
        left join
            {{ ref("dim_learner_first_success_response") }} as first_success
            on (
                first_success.org = events.org
                and first_success.course_key = events.course_key
                and first_success.object_id = events.object_id
            )
    )
select
    org,
    course_key,
    success,
    attempt,
    actor_id,
    problem_number,
    problem_name_location,
    block_id_short
from final_results
