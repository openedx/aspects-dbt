{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
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
            enrollment_status,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("enrollment_events") }}
    )

select org, course_key, actor_id, enrollment_status, enrollment_mode, emission_time
from ranked_enrollments
where rn = 1
