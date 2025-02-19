CREATE OR REPLACE DICTIONARY `xapi`.`course_tags` 
  
  (course_key String,tag String,course_name String,taxonomy_name String,lineage String)
  
    primary key (course_key, tag)
  SOURCE(
      CLICKHOUSE(
      user 'ch_admin'
      password 'ch_password'
      
        query "

select course_key, tag, course_name, taxonomy_name, lineage
from `xapi`.`most_recent_course_tags`
order by course_key"
      )

    )
  LAYOUT(COMPLEX_KEY_HASHED())
  LIFETIME(120)
