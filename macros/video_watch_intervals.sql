-- Pairs each `played` event with the next event for the same learner and video.
-- `chunks` splits by learner, so each learner's events stay in one chunk.
{% macro video_watch_intervals(min_emission_time, chunk=0, chunks=1) %}
    {%- set verb_played = "https://w3id.org/xapi/video/verbs/played" -%}
    {%- set verb_initialized = "http://adlnet.gov/expapi/verbs/initialized" -%}

    with
        events as (
            select
                event_id,
                org,
                course_key,
                actor_id,
                object_id,
                video_duration,
                emission_time_long,
                verb_id,
                video_position
            from {{ ref("video_playback_events") }}
            where
                verb_id != '{{ verb_initialized }}'
                and emission_time >= {{ min_emission_time }}
                {% if chunks > 1 -%}
                    and cityHash64(actor_id) % {{ chunks }} = {{ chunk }}
                {%- endif %}
            -- Unmerged duplicates would close a play with its own copy.
            limit 1 by event_id
        ),
        sequenced as (
            select
                *,
                leadInFrame(toNullable(event_id)) over w as next_event_id,
                leadInFrame(toNullable(video_position)) over w as next_video_position,
                leadInFrame(verb_id) over w as next_verb_id
            from events
            window
                w as (
                    partition by org, course_key, actor_id, object_id
                    -- On ties a closing event sorts before a play.
                    order by
                        emission_time_long asc,
                        verb_id = '{{ verb_played }}' asc,
                        event_id asc
                    rows between unbounded preceding and unbounded following
                )
        )
    select
        org,
        course_key,
        object_id,
        actor_id,
        event_id as start_event_id,
        emission_time_long as start_emission_time,
        video_position as start_video_position,
        next_video_position as end_video_position,
        next_verb_id as end_verb_id,
        video_duration,
        next_event_id is not null as is_interval_closed,
        ifNull(next_video_position > video_position, false) as is_watched,
        {{ video_watch_intervals_computed_at() }} as computed_at
    from sequenced
    where verb_id = '{{ verb_played }}'
{% endmacro %}

-- Inserts all of history one learner chunk at a time to cap memory.
-- The trailing `select 1` is because a post_hook needs a statement.
{% macro video_watch_intervals_backfill(relation) %}
    {%- set chunks = (
        env_var("ASPECTS_VIDEO_WATCH_INTERVALS_BACKFILL_CHUNKS", "16") | int
    ) -%}
    {%- for chunk in range(chunks) -%}
        {%- do run_query(
            "insert into "
            ~ relation
            ~ " "
            ~ video_watch_intervals("toDateTime(0)", chunk, chunks)
        ) -%}
    {%- endfor -%}
    select 1
{% endmacro %}

-- A macro so the unit test can pin it.
{% macro video_watch_intervals_computed_at() -%} now64(6) {%- endmacro %}
