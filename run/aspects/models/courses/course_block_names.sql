CREATE DICTIONARY `xapi`.`course_block_names__dbt_tmp` 
  
  (location String,block_name String,course_key String,graded Bool,course_order Int32,display_name_with_location String)
  
    primary key location
  SOURCE(
      CLICKHOUSE(
      user 'ch_admin'
      password 'ch_password'
      
        query "

select
    location, block_name, course_key, graded, course_order, display_name_with_location
from `xapi`.`most_recent_course_blocks`"
      )

    )
  LAYOUT(COMPLEX_KEY_SPARSE_HASHED())
  LIFETIME(120)
