-- Per learner and video, the distinct seconds watched and the most times any second was watched,
-- from the watched intervals. Instead of one row per second, it sweeps the interval ends: +1 where
-- an interval starts covering seconds, -1 after its last second, so the running total is how many
-- times each stretch was watched.
--
-- Only learner/video pairs with an interval starting at or after `min_emission_time` are
-- recomputed, from all of their intervals. `chunks` splits by learner to cap memory.
{% macro video_watches(min_emission_time, chunk=0, chunks=1) %}
    with
        changed as (
            select distinct org, course_key, object_id, actor_id
            from {{ ref("fact_video_watch_intervals") }}
            where
                start_emission_time >= {{ min_emission_time }}
                {% if chunks > 1 -%}
                    and cityHash64(actor_id) % {{ chunks }} = {{ chunk }}
                {%- endif %}
        ),
        intervals as (
            select
                org,
                course_key,
                object_id,
                actor_id,
                video_duration,
                -- Second n is playback from n - 1 to n.
                toInt64(start_video_position) + 1 as first_second,
                toInt64(assumeNotNull(end_video_position)) as last_second
            from {{ ref("fact_video_watch_intervals") }}
            where
                is_watched
                and (org, course_key, object_id, actor_id) in (select * from changed)
            settings final = 1
        ),
        sweeps as (
            select
                org,
                course_key,
                object_id,
                actor_id,
                any(video_duration) as video_duration,
                arraySort(
                    arrayConcat(
                        groupArray((first_second, 1)), groupArray((last_second + 1, -1))
                    )
                ) as points,
                arrayMap(p -> p.1, points) as positions,
                arrayCumSum(arrayMap(p -> p.2, points)) as depths
            from intervals
            group by org, course_key, object_id, actor_id
        )
    select
        org,
        course_key,
        object_id,
        actor_id,
        video_duration,
        toUInt64(
            arraySum(
                arrayMap(
                    i -> if(depths[i] > 0, positions[i + 1] - positions[i], 0),
                    arrayEnumerate(arrayPopBack(positions))
                )
            )
        ) as watched_seconds,
        toUInt64(arrayMax(depths)) as max_views,
        {{ video_watch_intervals_computed_at() }} as computed_at
    from sweeps
{% endmacro %}

-- Inserts every learner/video pair one learner chunk at a time to cap memory.
-- The trailing `select 1` is because a post_hook needs a statement.
{% macro video_watches_backfill(relation) %}
    {%- set chunks = (
        env_var("ASPECTS_VIDEO_WATCH_INTERVALS_BACKFILL_CHUNKS", "16") | int
    ) -%}
    {%- for chunk in range(chunks) -%}
        {%- do run_query(
            "insert into "
            ~ relation
            ~ " "
            ~ video_watches("toDateTime(0)", chunk, chunks)
        ) -%}
    {%- endfor -%}
    select 1
{% endmacro %}
