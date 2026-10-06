-- One row per `played` event and the event that closed it, if any.
--
-- A refreshable MV, not a plain one: a play and its closing event land in
-- different insert blocks, and a plain MV only sees one block (#176). Each
-- refresh re-pairs the lookback window; ReplacingMergeTree keeps the newest
-- row per start_event_id. Events older than the window, including late or
-- replayed ones, are only paired by the post_hook's full-history insert on the
-- next `dbt run`.
--
-- Post-hooks, in order:
-- 1. Refresh and wait, so a broken query fails `dbt run`. On a full refresh this
-- queues behind the refresh ClickHouse starts on creation instead of running
-- next to the back-fill.
-- 2. Back-fill all of history, in chunks (see video_watch_intervals_backfill).
-- catchup=False: the back-fill fills history. dbt-clickhouse names the view
-- `<model>_mv`.
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
