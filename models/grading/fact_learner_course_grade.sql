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
            org,
            course_key,
            actor_id,
            scaled_score as course_grade,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("fact_grades") }}
        where grade_type = 'course'
    )

select org, course_key, actor_id, course_grade
from ranked_grades
where rn = 1
