/*
TODO: verify if the order key is correct and will fix this issue:
group by
    -- multi-part questions include an extra record for the response to the first
    -- part of the question. this group by clause eliminates the duplicate record
*/
{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, problem_id)",
        order_by="(org, course_key, problem_id, actor_id, emission_time, responses, success, attempts, interaction_type)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    emission_time,
    org,
    course_key,
    object_id,
    {{ get_problem_id("object_id") }} as problem_id,
    actor_id,
    responses,
    success,
    attempts,
    interaction_type
    PROJECTION successful_responses
    (
        select
            org,
            course_key,
            problem_id,
            actor_id,
            min(emission_time) as first_success_at
        where
            success = true
        group by org, course_key, problem_id, actor_id
    )
    PROJECTION unsuccessful_responses
    (
        select
            org,
            course_key,
            problem_id,
            actor_id,
            max(emission_time) as last_response_at
        where actor_id not in (select distinct actor_id from successful_responses)
        group by org, course_key, problem_id, actor_id
    )
from {{ ref("problem_events") }}
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
