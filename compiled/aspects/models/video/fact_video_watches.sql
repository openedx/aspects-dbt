-- Per learner and video: distinct seconds watched and the most views of any second.


    with
        changed as (
            select distinct org, course_key, object_id, actor_id
            from `xapi`.`fact_video_watch_intervals`
            where
                start_emission_time >= now() - interval 1 DAY
                
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
            from `xapi`.`fact_video_watch_intervals`
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
                arrayMap(p -> tupleElement(p, 1), points) as positions,
                arrayCumSum(arrayMap(p -> tupleElement(p, 2), points)) as depths
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
        now64(6) as computed_at
    from sweeps
