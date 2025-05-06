{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

with
    final_results as (
        select
            max(emission_time) as emission_time_max,
            org,
            course_key,
            actor_id,
            argMax(enrollment_mode, emission_time) as enrollment_mode,
            argMax(enrollment_status, emission_time) as enrollment_status
        from {{ ref("enrollment_events") }}
        group by org, course_key, actor_id
    )
select
    org,
    course_key,
    actor_id,
    enrollment_status,
    enrollment_mode,
    emission_time_max as emission_time
from final_results
