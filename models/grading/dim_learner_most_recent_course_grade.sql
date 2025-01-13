{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
    )
}}

with
    ranked_grades as (
        select
            emission_time,
            org,
            course_key,
            actor_id,
            scaled_score as course_grade,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("grading_events") }}
        where object_id like '%/course/%'
    )

select org, course_key, actor_id, course_grade, emission_time
from ranked_grades
where rn = 1
