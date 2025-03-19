CREATE OR REPLACE DICTIONARY `xapi`.`dim_course_names` 
  
  (course_key String,course_name String,course_run String,org String,tags_str String)
  
    primary key course_key
  SOURCE(
      CLICKHOUSE(
      user 'ch_admin'
      password 'ch_password'
      
        query "

with
    latest as (
        select org, course_key, max(modified) as last_modified
        from `event_sink`.`course_overviews`
        group by org, course_key
    )
select
    course_key,
    display_name as course_name,
    splitByString('+', course_key)[-1] as course_run,
    org,
    JSONExtract(course_data_json, 'tags', 'String') as tags_str
from `event_sink`.`course_overviews` co
inner join
    latest mr
    on mr.org = co.org
    and mr.course_key = co.course_key
    and co.modified = mr.last_modified"
      )

    )
  LAYOUT(COMPLEX_KEY_HASHED())
  LIFETIME(120)
