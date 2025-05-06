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
            max(emission_time) as emission_time_max,
            org,
            course_key,
            actor_id,
            argMax(scaled_score, emission_time) as course_grade
        from {{ ref("grading_events") }}
        where object_id like '%/course/%'
        group by org, course_key, actor_id
    )
select org, course_key, actor_id, course_grade, emission_time_max as emission_time
from final_results
