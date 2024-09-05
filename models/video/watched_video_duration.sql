{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org,course_key,actor_id,video_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

with
    starts as (
        select org,course_key,actor_id,emission_time, cast(video_position as Int32) as start_position, 
        splitByString('/xblock/', object_id)[-1] as video_id, video_duration
        from {{ ref('video_playback_events') }}
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    )    
    , rewatches as (
      select org, course_key, video_id, actor_id, start_position
      from starts
      group by org, course_key, video_id, actor_id, start_position
      having count(1) > 1
    )
    , ends as (
        select org,course_key,actor_id,emission_time, cast(video_position as Int32) as end_position, 
        splitByString('/xblock/', object_id)[-1] as video_id
        from {{ ref('video_playback_events') }}
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/seeked',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated'
            )
    ), subtotals as (
        select 
        starts.org as org, 
        starts.course_key as course_key,
        starts.actor_id as actor_id,
        starts.video_id as video_id,
        starts.video_duration,
        sum(starts.start_position - end_position) as duration,
        case when rewatches.org = '' then duration else 0 end as watched_time,
        case when rewatches.org <> '' then duration else 0 end as rewatched_time
        from starts
        left asof join ends on 
            starts.org = ends.org
            and starts.course_key = ends.course_key
            and starts.video_id = ends.video_id
            and starts.actor_id = ends.actor_id
            and starts.emission_time < ends.emission_time
        left join rewatches on 
            rewatches.org = starts.org and 
            rewatches.course_key = starts.course_key and 
            rewatches.actor_id = starts.actor_id and 
            rewatches.start_position = starts.start_position AND
            rewatches.video_id = starts.video_id
        group by org, course_key, actor_id, video_id, video_duration, rewatches.org
    )
      select org, course_key, actor_id, video_id, video_duration, sum(watched_time) as watched_time, sum(rewatched_time) as rewatched_time
      from subtotals
      group by org, course_key, actor_id, video_id, video_duration
