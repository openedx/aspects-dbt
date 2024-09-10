{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org,course_key,actor_id,video_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

WITH starts AS (
    SELECT 
        org,
        course_key,
        actor_id,
        emission_time,
        cast(video_position AS Int32) AS start_position,
        splitByString('/xblock/', object_id)[-1] AS video_id,
        video_duration
   FROM {{ ref("video_playback_events") }}
   WHERE verb_id IN ('https://w3id.org/xapi/video/verbs/played') 
), 
ends AS (
    SELECT 
        org,
        course_key,
        actor_id,
        emission_time,
        cast(video_position AS Int32) AS end_position,
        splitByString('/xblock/', object_id)[-1] AS video_id
   FROM {{ ref("video_playback_events") }}
   WHERE verb_id IN ('http://adlnet.gov/expapi/verbs/completed',
                     'https://w3id.org/xapi/video/verbs/paused',
                     'http://adlnet.gov/expapi/verbs/terminated',
                     'https://w3id.org/xapi/video/verbs/seeked') 
),
range_multi AS (
    SELECT 
        starts.org AS org,
        starts.course_key AS course_key,
        starts.actor_id AS actor_id,
        starts.video_id AS video_id,
        starts.video_duration AS video_duration,
        starts.start_position AS start_position,
        ends.end_position AS end_position,
        generateUUIDv4() AS range_id,
        starts.emission_time AS start_emission_time,
        ends.emission_time AS end_emission_time,
        row_number() OVER (PARTITION BY org,course_key,actor_id,video_id,start_position ORDER BY ends.emission_time) AS rownum
   FROM starts
   LEFT JOIN ends ON starts.org = ends.org
       AND starts.course_key = ends.course_key
       AND starts.video_id = ends.video_id
       AND starts.actor_id = ends.actor_id
   WHERE starts.emission_time < ends.emission_time 
),
range AS (SELECT * FROM range_multi WHERE rownum = 1
), 
rewatched AS (
    SELECT 
        org,
        course_key,
        actor_id,
        video_id,
        video_duration,
        a.range_id AS range_id1,
        b.range_id AS range_id2,
        a.end_position - a.start_position AS duration1,
        b.end_position - b.start_position AS duration2
   FROM range a
   INNER JOIN range b ON a.org = b.org
       AND a.course_key = b.course_key
       AND a.actor_id = b.actor_id
       AND a.video_id = b.video_id
   WHERE (
      (b.start_position > a.start_position AND b.start_position < a.end_position)
      OR (b.end_position > a.start_position AND b.end_position < a.end_position)
    )
    AND b.start_emission_time > a.start_emission_time 
)
SELECT 
    org,
    course_key,
    actor_id,
    video_id,
    video_duration,
    sum(watched_time) AS watched_time,
    sum(rewatched_time) AS rewatched_time
FROM (
    SELECT 
        org,
        course_key,
        actor_id,
        video_id,
        video_duration,
        sum(end_position - start_position) AS watched_time,
        0 AS rewatched_time
    FROM range
    WHERE range.range_id NOT IN (SELECT range_id1 FROM rewatched)
      AND range.range_id NOT IN (SELECT range_id2 FROM rewatched)
    GROUP BY org, course_key, actor_id, video_id, video_duration
    UNION ALL 
    SELECT 
        org,
        course_key,
        actor_id,
        video_id,
        video_duration,
        0 AS watched_time,
        sum(duration1 + duration2) AS rewatched_time
   FROM rewatched
   GROUP BY org, course_key, actor_id, video_id, video_duration
)
GROUP BY org, course_key, actor_id, video_id, video_duration
