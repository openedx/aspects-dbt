{{ config(
    materialized='materialized_view',
    engine=get_engine('ReplacingMergeTree()'),
    primary_key='(org, course_key, verb_id)',
    order_by='(org, course_key, verb_id, emission_time, actor_id, object_id, scaled_score, event_id)'
  ) }}


SELECT
    event_id,
    CAST(emission_time, 'DateTime') AS emission_time,
    actor_id,
    object_id,
    splitByString('/', course_id)[-1] AS course_key,
    org,
    verb_id,
    JSONExtractFloat(event_str, 'result', 'score', 'scaled') AS scaled_score
FROM {{ ref('xapi_events_all_parsed') }}
WHERE verb_id IN ('http://id.tincanapi.com/verb/earned', 'https://w3id.org/xapi/acrossx/verbs/evaluated')