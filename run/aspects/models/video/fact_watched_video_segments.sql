

  create view `xapi`.`fact_watched_video_segments` 
  
    
    
  as (
    with
    video_events as (
        select
            emission_time,
            org,
            course_key,
            splitByString('/xblock/', object_id)[-1] as video_id,
            actor_id,
            verb_id,
            video_position,
            video_duration
        from `xapi`.`video_playback_events`
    ),
    starts as (
        select *
        from video_events
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    ),
    ends as (
        select *
        from video_events
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/completed',
                'https://w3id.org/xapi/video/verbs/seeked',
                'https://w3id.org/xapi/video/verbs/paused',
                'http://adlnet.gov/expapi/verbs/terminated'
            )
    ),
    segments as (
        select
            starts.org as org,
            starts.course_key as course_key,
            starts.video_id as video_id,
            starts.actor_id,
            cast(starts.video_position as Int32) as start_position,
            cast(ends.video_position as Int32) as end_position,
            starts.emission_time as started_at,
            ends.emission_time as ended_at,
            ends.verb_id as end_type,
            starts.video_duration as video_duration
        from starts left
        asof join
            ends
            on (
                starts.org = ends.org
                and starts.course_key = ends.course_key
                and starts.video_id = ends.video_id
                and starts.actor_id = ends.actor_id
                and starts.emission_time < ends.emission_time
            )
    ),
    enriched_segments as (
        select
            segments.org as org,
            segments.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            blocks.section_with_name as section_with_name,
            blocks.subsection_with_name as subsection_with_name,
            blocks.block_name as video_name,
            blocks.display_name_with_location as video_name_with_location,
            segments.actor_id as actor_id,
            segments.started_at as started_at,
            segments.start_position - (segments.start_position % 5) as start_position,
            segments.end_position - (segments.end_position % 5) as end_position,
            segments.video_duration as video_duration
        from segments
        join
            `xapi`.`dim_course_blocks_extended` blocks
            on (
                segments.course_key = blocks.course_key
                and segments.video_id = blocks.block_id
            )
    )

select
    org,
    course_key,
    course_name,
    course_run,
    section_with_name,
    subsection_with_name,
    video_name,
    video_name_with_location,
    actor_id,
    started_at,
    arrayJoin(range(start_position, end_position, 5)) as segment_start,
    video_duration,
    CONCAT(toString(segment_start), '-', toString(segment_start + 4)) as segment_range,
    start_position,
    username,
    name,
    email
from enriched_segments
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
order by start_position
  )
      
      
                    -- end_of_sql
                    
                    