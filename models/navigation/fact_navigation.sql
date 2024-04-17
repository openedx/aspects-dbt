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
    navigation.ending_point as ending_point,
    users.username as username,
    users.name as name,
    users.email as email
from {{ ref("navigation_events") }} navigation
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        navigation.course_key = blocks.course_key
        and navigation.block_id = blocks.block_id
    )
left outer join
    {{ ref("dim_user_pii") }} users on toUUID(actor_id) = users.external_user_id
