-- One row per learner and unit they navigated away from, with the latest visit. Kept up to date
-- as events arrive, so engagement reports don't regroup every navigation event.
{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree(emission_time)"),
        order_by="(org, course_key, actor_id, block_id)",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select org, course_key, actor_id, block_id, emission_time
from {{ ref("navigation_events") }}
