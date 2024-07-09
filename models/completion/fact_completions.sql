with
    completions as (
        select
            emission_time,
            org,
            course_key,
            actor_id,
            progress_percent,
            if(
                object_id like '%/course/%',
                splitByString('/course/', object_id)[-1],
                splitByString('/xblock/', object_id)[-1]
            ) as entity_id,
            cast(progress_percent as Float) / 100 as scaled_progress
        from {{ ref("completion_events") }}
    )

select
    completions.emission_time as emission_time,
    completions.org as org,
    completions.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    completions.entity_id as entity_id,
    if(blocks.block_name != '', blocks.block_name, courses.course_name) as entity_name,
    if(
        blocks.block_name != '', blocks.display_name_with_location, null
    ) as entity_name_with_location,
    completions.actor_id as actor_id,
    cast(completions.scaled_progress as Float) as scaled_progress,
    get_bucket(scaled_progress) as completion_bucket,
    users.username as username,
    users.name as name,
    users.email as email
from completions
join {{ ref("course_names") }} courses on completions.course_key = courses.course_key
left join
    {{ ref("course_block_names") }} blocks on completions.entity_id = blocks.location
left outer join
    {{ ref("dim_user_pii") }} users
    on toUUID(completions.actor_id) = users.external_user_id
