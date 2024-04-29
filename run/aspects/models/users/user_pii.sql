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
        select
            user_id,
            name,
            email,
            ROW_NUMBER() over (
                partition by user_id order by (id, time_last_dumped) DESC
            ) as rn
        from `event_sink`.`user_profile`
    )
select mrup.user_id as user_id, external_user_id, username, name, email
from `event_sink`.`external_id` ex
left outer join most_recent_user_profile mrup on mrup.user_id = ex.user_id
where mrup.rn = 1"
      )

    )
  LAYOUT(COMPLEX_KEY_SPARSE_HASHED())
  LIFETIME(120)
