

  create view `xapi`.`fact_navigation` 
  
    
    
  as (
    select
    navigation.emission_time as emission_time,
    navigation.org as org,
    navigation.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    navigation.actor_id as actor_id,
    navigation.block_id as block_id,
    blocks.block_name as block_name,
    blocks.display_name_with_location as block_name_with_location,
    navigation.object_type as object_type,
    navigation.starting_position as starting_position,
    navigation.ending_point as ending_point
from `xapi`.`navigation_events` navigation
join
    `xapi`.`dim_course_blocks` blocks
    on (
        navigation.course_key = blocks.course_key
        and navigation.block_id = blocks.block_id
    )
  )
      
      
                    -- end_of_sql
                    
                    