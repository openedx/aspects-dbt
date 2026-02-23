

with
    most_recent_user_profile as (
        select user_id, max(time_last_dumped) as time_last_dumped
        from `event_sink`.`user_profile`
        group by user_id
    )
select
    external_id.user_id as user_id,
    if(
        empty(external_id.external_user_id),
        concat('mailto:', user_profile.email),
        external_id.external_user_id::String
    ) as external_user_id,
    user_profile.username as username,
    user_profile.name as name,
    user_profile.email as email
from most_recent_user_profile
left outer join
    `event_sink`.`external_id` external_id
    on most_recent_user_profile.user_id = external_id.user_id
left outer join
    `event_sink`.`user_profile` user_profile
    on user_profile.user_id = most_recent_user_profile.user_id
    and user_profile.time_last_dumped = most_recent_user_profile.time_last_dumped