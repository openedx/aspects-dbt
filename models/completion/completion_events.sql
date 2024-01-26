{{ config(
    materialized='materialized_view',
    schema=env_var('ASPECTS_XAPI_DATABASE', 'xapi'),
    engine=get_engine('ReplacingMergeTree()'),
    primary_key='(org, course_key, verb_id)',
    order_by='(org, course_key, verb_id, emission_time, actor_id, object_id, event_id)',
    partition_by='(toYYYYMM(emission_time))',
  ) }}

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] AS course_key,
    org,
    verb_id,
    JSON_VALUE(event, '$.result.extensions."https://w3id.org/xapi/cmi5/result/extensions/progress"') AS progress_percent
FROM {{ ref('xapi_events_all_parsed') }}
WHERE verb_id = 'http://adlnet.gov/expapi/verbs/progressed'
