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
from {{ ref("problem_events") }}
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
