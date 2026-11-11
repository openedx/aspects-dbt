-- One row per learner x competency with its latest mastery status.
-- ReplacingMergeTree(time_last_dumped) keeps the latest dump per key only after a
-- merge: read with FINAL.
{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree(time_last_dumped)"),
        order_by="(tag_id, actor_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
select
    id,
    actor_id,
    tag_id,
    taxonomy_id,
    status_id,
    status_name,
    parseDateTime64BestEffort(created, 6) as created,
    parseDateTime64BestEffort(modified, 6) as modified,
    parseDateTime64BestEffort(time_last_dumped, 6) as time_last_dumped
from {{ source("event_sink", "student_competency_status") }}
