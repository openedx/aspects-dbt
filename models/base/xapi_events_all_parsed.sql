{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_id, verb_id, actor_id, emission_time, event_id)",
        order_by="(org, course_id, verb_id, actor_id, emission_time, event_id)",
        partition_by="(toYYYYMM(emission_time))",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

select
    event_id as event_id,
    toLowCardinality(JSON_VALUE(event::String, '$.verb.id')) as verb_id,
    COALESCE(
        NULLIF(JSON_VALUE(event::String, '$.actor.account.name'), ''),
        NULLIF(JSON_VALUE(event::String, '$.actor.mbox'), ''),
        JSON_VALUE(event::String, '$.actor.mbox_sha1sum')
    ) as actor_id,
    JSON_VALUE(event::String, '$.object.id') as object_id,
    -- If the contextActivities parent is a course, use that. It can be a "course"
    -- type, or a "cmi.interaction" type for multiple question problem submissions.
    -- Otherwise use the object id for the course id.
    toLowCardinality(
        multiIf(
            -- If the contextActivities parent is a course, use that
            JSON_VALUE(
                event::String, '$.context.contextActivities.parent[0].definition.type'
            )
            = 'http://adlnet.gov/expapi/activities/course',
            JSON_VALUE(event::String, '$.context.contextActivities.parent[0].id'),
            -- Else if the contextActivities parent is a GroupActivity, it's a multi
            -- question problem and we use the grouping id
            JSON_VALUE(
                event::String, '$.context.contextActivities.parent[0].objectType'
            )
            in ('Activity', 'GroupActivity'),
            JSON_VALUE(event::String, '$.context.contextActivities.grouping[0].id'),
            -- Otherwise use the object id
            JSON_VALUE(event::String, '$.object.id')
        )
    ) as course_id,
    toLowCardinality(splitByString('/', course_id)[-1]) as course_key,
    toLowCardinality(
        coalesce(
            get_org_from_course_url(course_id),
            get_org_from_ccx_course_url(course_id),
            ''
        )
    ) as org,
    emission_time as emission_time,
    event::String as event
from {{ source("xapi", "xapi_events_all") }}
