{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, problem_id, actor_id)",
    )
}}

select
    org,
    course_key,
    object_id,
    problem_id,
    actor_id,
    interaction_type,
    MAX(attempts) as attempt,
    argMax(success, attempts) as success,
    argMax(emission_time, attempts) as emission_time,
    argMax(responses, attempts) as responses,
    argMax(scaled_score, attempts) as scaled_score
from {{ ref("problem_events") }}
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
group by org, course_key, object_id, problem_id, actor_id, interaction_type
