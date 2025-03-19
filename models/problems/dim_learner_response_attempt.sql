-- select one record per (learner, problem, course, org) tuple
-- contains either the first successful attempt
-- or the most recent unsuccessful attempt
-- find the timestamp of the earliest successful response
-- this will be used to pick the xAPI event corresponding to that submission
{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, problem_id)",
        order_by="(org, course_key, problem_id, actor_id)",
        partition_by="toYYYYMM(emission_time)",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

with
    responses as (
        select
            emission_time,
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            success,
            responses,
            attempts,
            interaction_type
        from {{ ref("problem_events") }}
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    ),
    response_status as (
        select
            org,
            course_key,
            problem_id,
            actor_id,
            MIN(case when success then emission_time else NULL end) as first_success_at,
            MAX(emission_time) as last_attempt_at
        from responses
        group by org, course_key, problem_id, actor_id
    )
select
    org,
    course_key,
    problem_id,
    actor_id,
    first_success_at,
    last_attempt_at,
    coalesce(first_success_at, last_attempt_at) as emission_time
from response_status
