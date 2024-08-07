CREATE DICTIONARY `xapi`.`user_pii__dbt_tmp` 
  
  (user_id Int32,external_user_id UUID,username String,name String,email String)
  
    primary key (user_id, external_user_id)
  SOURCE(
      CLICKHOUSE(
      user 'ch_admin'
      password 'ch_password'
      
        query "

with
    most_recent_user_profile as (
        select user_id, max(time_last_dumped) as time_last_dumped
        from `event_sink`.`user_profile`
        group by user_id
    )
select ex.user_id as user_id, ex.external_user_id, ex.username, up.name, up.email
from `event_sink`.`external_id` ex
left outer join most_recent_user_profile mrup on mrup.user_id = ex.user_id
left outer join
    `event_sink`.`user_profile` up
    on up.user_id = mrup.user_id
    and up.time_last_dumped = mrup.time_last_dumped"
      )

    )
  LAYOUT(COMPLEX_KEY_SPARSE_HASHED())
  LIFETIME(120)
