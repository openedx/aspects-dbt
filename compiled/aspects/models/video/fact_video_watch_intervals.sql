-- One row per `played` event and the event that closed it, if any.

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
            from `xapi`.`video_playback_events`
            where
                verb_id != 'http://adlnet.gov/expapi/verbs/initialized'
                and emission_time >= now() - interval 1 DAY
                
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
                        verb_id = 'https://w3id.org/xapi/video/verbs/played' asc,
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
        now64(6) as computed_at
    from sequenced
    where verb_id = 'https://w3id.org/xapi/video/verbs/played'
