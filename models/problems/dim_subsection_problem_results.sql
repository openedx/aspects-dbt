{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(course_key, block_id_short, actor_id, problem_id)",
        order_by="(course_key, block_id_short, actor_id, problem_id)",
    )
}}

select
    org,
    course_key,
    block_id_short,
    problem_id,  -- for primary key
    problem_location,
    problem_name_location,
    actor_id,
    success,
    subsection_number,
    subsection_with_name,
    scaled_score
from {{ ref("dim_subsection_problem_results_pre") }}
where graded
