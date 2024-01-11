{{ config(
    materialized='materialized_view',
    schema=env_var('ASPECTS_XAPI_DATABASE', 'xapi'),
    engine=get_engine('ReplacingMergeTree()'),
    primary_key='(org, course_key)',
    order_by='(org, course_key, emission_time, actor_id, enrollment_mode, event_id)'
  ) }}

SELECT
    event_id,
    cast(emission_time as DateTime) as emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] AS course_key,
    org,
    verb_id,
    JSON_VALUE(event_str, '$.object.definition.extensions."https://w3id.org/xapi/acrossx/extensions/type"') AS enrollment_mode
FROM {{ ref('xapi_events_all_parsed') }}
WHERE verb_id IN (
    'http://adlnet.gov/expapi/verbs/registered',
    'http://id.tincanapi.com/verb/unregistered'
)
