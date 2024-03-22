{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
    )
}}

with
    ranked_enrollments as (
        select
            emission_time,
            org,
            course_key,
            actor_id,
            enrollment_mode,
            splitByString('/', verb_id)[-1] as enrollment_status,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("enrollment_events") }}
    )

select org, course_key, actor_id, enrollment_status
from ranked_enrollments
where rn = 1
