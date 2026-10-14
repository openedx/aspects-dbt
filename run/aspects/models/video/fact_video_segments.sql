

  create or replace view `xapi`.`fact_video_segments` 
  
    
  
  
    
    
  as (
    -- One row per second of each watched interval; sum watch_count to count views.


-- `settings final = 1` instead of FINAL, which breaks the unit test fixture.
select
    org,
    course_key,
    actor_id,
    object_id,
    video_duration,
    -- Second n is playback from n - 1 to n. ifNull: ClickHouse may evaluate this before
    -- the is_watched filter has removed the open intervals.
    arrayJoin(
        range(
            cast(start_video_position as int) + 1,
            cast(ifNull(end_video_position, 0) as int) + 1,
            1
        )
    ) as watched_segment,
    toUInt64(1) as watch_count
from `xapi`.`fact_video_watch_intervals` as intervals
where intervals.is_watched settings final = 1
    
  )
      
      
                    -- end_of_sql
                    
                    