-- One row per `played` event and the event that closed it, if any.
{%- set lookback = env_var("ASPECTS_VIDEO_WATCH_INTERVALS_LOOKBACK", "1 DAY") -%}
{%- set view = "{{ this.schema }}.{{ this.identifier }}_mv" -%}

{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree(computed_at)"),
        primary_key="(org, course_key, object_id, actor_id)",
        order_by="(org, course_key, object_id, actor_id, start_event_id)",
        refreshable={
            "interval": env_var(
                "ASPECTS_VIDEO_WATCH_INTERVALS_REFRESH", "EVERY 5 MINUTE"
            ),
            "randomize": env_var(
                "ASPECTS_VIDEO_WATCH_INTERVALS_REFRESH_RANDOMIZE", "1 MINUTE"
            ),
            "append": True,
        },
        catchup=False,
        post_hook=[
            "system refresh view " ~ view,
            "system wait view " ~ view,
            "{{ video_watch_intervals_backfill(this) }}",
        ],
    )
}}

{{ video_watch_intervals(min_emission_time="now() - interval " ~ lookback) }}
