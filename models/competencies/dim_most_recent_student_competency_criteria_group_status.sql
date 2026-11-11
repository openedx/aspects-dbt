-- One row per learner x criteria group with its latest mastery status. Course-level
-- groups (group_level = 'course') give the learner's status for a competency in one
-- course, the only grain where AttemptedNotDemonstrated exists.
{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree(time_last_dumped)"),
        order_by="(id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
select
    id,
    actor_id,
    group_id,
    parent_group_id,
    tag_id,
    taxonomy_id,
    course_key,
    group_level,
    status_id,
    status,
    parseDateTime64BestEffort(created, 6) as created,
    parseDateTime64BestEffort(modified, 6) as modified,
    parseDateTime64BestEffort(time_last_dumped, 6) as time_last_dumped
from {{ source("event_sink", "student_competency_criteria_group_status") }}
