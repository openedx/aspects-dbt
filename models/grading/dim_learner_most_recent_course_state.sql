{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, actor_id)",
        order_by="(org, course_key, actor_id)",
    )
}}

with
    grade as (
        select
            org,
            course_key,
            actor_id,
            argMax(scaled_score, emission_time) as course_grade
        from {{ ref("grading_events") }}
        where
            object_id like '%/course/%'
            and (
                (
                    verb_id = 'http://adlnet.gov/expapi/verbs/passed'
                    and scaled_score <> 0
                )
                or (verb_id <> 'http://adlnet.gov/expapi/verbs/passed')
            )
        group by org, course_key, actor_id
    ),
    state as (
        select
            org,
            course_key,
            actor_id,
            argMax(approving_state, emission_time) as _approving_state
        from {{ ref("grading_events") }}
        where not empty(approving_state)
        group by org, course_key, actor_id
    ),
    actors as (
        select org, course_key, actor_id
        from grade
        union distinct
        select org, course_key, actor_id
        from state
    )
select
    actors.org as org,
    actors.course_key as course_key,
    actors.actor_id as actor_id,
    grade.course_grade as course_grade,
    state._approving_state as approving_state
from actors
left join
    grade
    on actors.org = grade.org
    and actors.course_key = grade.course_key
    and actors.actor_id = grade.actor_id
left join
    state
    on actors.org = state.org
    and actors.course_key = state.course_key
    and actors.actor_id = state.actor_id
