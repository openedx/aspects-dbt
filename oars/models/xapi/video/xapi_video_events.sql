select event_id, verb_id, actor_id, org, course_id, object_id, emission_time,
        event.result.extensions.'https://w3id.org/xapi/video/extensions/time' as video_action_time_secs,
        coalesce(event.result.extensions.'https://w3id.org/xapi/video/extensions/cc-enabled', null) as captions_enabled,
        coalesce(event.context.extensions.'https://w3id.org/xapi/openedx/extension/transformer-version', null) as xapi_transformer_version,
        coalesce(event.context.extensions.'https://w3id.org/xapi/openedx/extensions/session-id', null) as session_id,
        coalesce(event.context.extensions.'https://w3id.org/xapi/video/extensions/length', null) as video_length_secs
from {{ source('xapi_raw', 'xapi_events_all_parsed') }}
where object_id like '%video+block%'
