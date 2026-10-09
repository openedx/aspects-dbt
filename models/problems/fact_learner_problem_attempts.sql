-- One row per learner and problem they submitted, with the latest submission. Kept up to date as
-- events arrive, so engagement reports don't regroup every problem event.
{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree(emission_time)"),
        order_by="(org, course_key, actor_id, problem_id)",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select org, course_key, actor_id, problem_id, emission_time
from {{ ref("problem_events") }}
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
