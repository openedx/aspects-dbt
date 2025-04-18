{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
    )
}}

with
    final_results as (
        select
            org,
            course_key,
            actor_id,
            argMax(approving_state, emission_time) as approving_state_max,
            max(emission_time) as emission_time_max
        from {{ ref("grading_events") }}
        where not empty(approving_state)
        group by org, course_key, actor_id
    )
select org, course_key, actor_id, approving_state_max as approving_state, emission_time_max as emission_time
from final_results
