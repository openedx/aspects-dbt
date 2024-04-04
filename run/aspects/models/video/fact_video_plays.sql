

  create view `xapi`.`fact_video_plays` 
  
    
    
  as (
    -- model to support number of watches per video
-- ref:
-- https://edx.readthedocs.io/projects/edx-insights/en/latest/Reference.html#engagement-computations
with
    plays as (
        select
            emission_time,
            org,
            course_key,
            video_duration,
            video_position,
            splitByString('/xblock/', object_id)[-1] as video_id,
            actor_id
        from `xapi`.`video_playback_events`
        where verb_id = 'https://w3id.org/xapi/video/verbs/played'
    )

select
    plays.emission_time as emission_time,
    plays.org as org,
    plays.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    plays.video_id as video_id,
    blocks.block_name as video_name,
    blocks.display_name_with_location as video_name_with_location,
    video_position,
    video_duration,
    case
        when video_position/video_duration >= 0.9
        then '90-100%'
        when video_position/video_duration >= 0.8 and video_position/video_duration < 0.9
        then '80-89%'
        when video_position/video_duration >= 0.7 and video_position/video_duration < 0.8
        then '70-79%'
        when video_position/video_duration >= 0.6 and video_position/video_duration < 0.7
        then '60-69%'
        when video_position/video_duration >= 0.5 and video_position/video_duration < 0.6
        then '50-59%'
        when video_position/video_duration >= 0.4 and video_position/video_duration < 0.5
        then '40-49%'
        when video_position/video_duration >= 0.3 and video_position/video_duration < 0.4
        then '30-39%'
        when video_position/video_duration >= 0.2 and video_position/video_duration < 0.3
        then '20-29%'
        when video_position/video_duration >= 0.1 and video_position/video_duration < 0.2
        then '10-19%'
        else '0-9%'
    end as visualization_bucket,
    plays.actor_id as actor_id
from plays
join
    `xapi`.`dim_course_blocks` blocks
    on (plays.course_key = blocks.course_key and plays.video_id = blocks.block_id)
  )
      
      
                    -- end_of_sql
                    
                    