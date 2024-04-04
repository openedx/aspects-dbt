{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
    )
}}

with
    ranked_status as (
        select
            org,
            course_key,
            actor_id,
            splitByString('/', verb_id)[-1] as approving_state,
            emission_time,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("grading_events") }}
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/passed',
                'http://adlnet.gov/expapi/verbs/failed'
            )
    )

select org, course_key, actor_id, approving_state, emission_time
from ranked_status
where rn = 1
