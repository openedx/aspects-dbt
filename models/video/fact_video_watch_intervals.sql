-- One row per `played` event and the event that closed it, if any.
--
-- A refreshable MV, not a plain one: a play and its closing event land in
-- different insert blocks, and a plain MV only sees one block (#176). Each
-- refresh re-pairs the lookback window; ReplacingMergeTree keeps the newest
-- row per start_event_id. Events older than the window, including late or
-- replayed ones, are only paired by the post_hook's full-history insert on the
-- next `dbt run`.
--
-- catchup=False: the post_hook already fills history.
-- The forced refresh + wait makes a broken refresh query fail `dbt run`.
-- dbt-clickhouse names the refreshable view `<model>_mv`.
{%- set lookback = env_var("ASPECTS_VIDEO_WATCH_INTERVALS_LOOKBACK", "1 DAY") -%}

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
            "insert into {{ this }} {{ video_watch_intervals(min_emission_time='toDateTime(0)') }}",
            "system refresh view {{ this.schema }}.{{ this.identifier }}_mv",
            "system wait view {{ this.schema }}.{{ this.identifier }}_mv",
        ],
    )
}}

{{ video_watch_intervals(min_emission_time="now() - interval " ~ lookback) }}
