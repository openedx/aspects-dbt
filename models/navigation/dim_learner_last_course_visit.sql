{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select org, course_key, actor_id, max(emission_time) as emission_time
from {{ ref("navigation_events") }}
group by org, course_key, actor_id
