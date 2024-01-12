{{ config(
    materialized='materialized_view',
    schema=env_var('ASPECTS_XAPI_DATABASE', 'xapi'),
    engine=get_engine('ReplacingMergeTree()'),
    primary_key='(org, course_key, verb_id)',
    order_by='(org, course_key, verb_id, emission_time, actor_id, object_id, event_id)'
  ) }}

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    org,
    splitByString('/', course_id)[-1] AS course_key,
    object_id,
    actor_id,
    verb_id
FROM {{ ref('xapi_events_all_parsed') }}
WHERE JSON_VALUE(event_str, '$.object.definition.type') = 'http://id.tincanapi.com/activitytype/discussion'
