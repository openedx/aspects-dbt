

with
    most_recent_user_profile as (
        select user_id, max(time_last_dumped) as time_last_dumped
        from `event_sink`.`user_profile`
        group by user_id
    )
select
    ex.user_id as user_id,
    if(
        empty(ex.external_user_id),
        concat('mailto:', email),
        ex.external_user_id::String
    ) as external_user_id,
    up.username as username,
    up.name as name,
    up.email as email
from most_recent_user_profile mrup
left outer join
    `event_sink`.`external_id` ex on mrup.user_id = ex.user_id
left outer join
    `event_sink`.`user_profile` up
    on up.user_id = mrup.user_id
    and up.time_last_dumped = mrup.time_last_dumped