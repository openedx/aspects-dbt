{{ config(
    materialized='materialized_view',
    schema=env_var('ASPECTS_XAPI_DATABASE', 'xapi'),
    engine=get_engine('ReplacingMergeTree()'),
    primary_key='(org, course_key, video_id)',
    order_by='(org, course_key, video_id, emission_time, actor_id, cc_enabled, event_id)',
    partition_by='(toYYYYMM(emission_time))',
  ) }}

SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    org,
    splitByString('/', course_id)[-1] AS course_key,
    splitByString('/xblock/', object_id)[2] as video_id,
    actor_id,
    JSONExtractBool(event, 'result','extensions','https://w3id.org/xapi/video/extensions/cc-enabled') as cc_enabled
FROM {{ ref('xapi_events_all_parsed') }}
WHERE
    verb_id IN ('http://adlnet.gov/expapi/verbs/interacted')
    AND JSONHas(event, 'result', 'extensions', 'https://w3id.org/xapi/video/extensions/cc-enabled')
