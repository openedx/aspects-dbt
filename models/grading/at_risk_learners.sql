{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
    )
}}

with
    page_visits as (
        select org, course_key, actor_id, max(emission_time) as last_visited
        from {{ ref('fact_learner_last_course_visit') }}
        where emission_time < subtractDays(now(), 7)
        group by org, course_key, actor_id
    )
select org, course_key, learners.actor_id as actor_id
from {{ ref('fact_student_status') }} learners
join page_visits using (org, course_key, actor_id)
where approving_state = 'failed' and enrollment_status = 'registered'
