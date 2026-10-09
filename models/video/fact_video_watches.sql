-- Per learner and video: distinct seconds watched and the most views of any second.
{%- set lookback = env_var("ASPECTS_VIDEO_WATCH_INTERVALS_LOOKBACK", "1 DAY") -%}
{%- set view = "{{ this.schema }}.{{ this.identifier }}_mv" -%}

{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree(computed_at)"),
        order_by="(org, course_key, object_id, actor_id)",
        refreshable={
            "interval": env_var(
                "ASPECTS_VIDEO_WATCH_INTERVALS_REFRESH", "EVERY 5 MINUTE"
            ),
            "depends_on": [this.schema ~ ".fact_video_watch_intervals_mv"],
            "append": True,
        },
        catchup=False,
        post_hook=[
            "{{ video_watches_backfill(this) }}",
            "system refresh view " ~ view,
            "system wait view " ~ view,
        ],
    )
}}

{{ video_watches(min_emission_time="now() - interval " ~ lookback) }}
