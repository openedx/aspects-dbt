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
    case
        when scaled_progress >= 0.9
        then '90-100%'
        when scaled_progress >= 0.8 and scaled_progress < 0.9
        then '80-89%'
        when scaled_progress >= 0.7 and scaled_progress < 0.8
        then '70-79%'
        when scaled_progress >= 0.6 and scaled_progress < 0.7
        then '60-69%'
        when scaled_progress >= 0.5 and scaled_progress < 0.6
        then '50-59%'
        when scaled_progress >= 0.4 and scaled_progress < 0.5
        then '40-49%'
        when scaled_progress >= 0.3 and scaled_progress < 0.4
        then '30-39%'
        when scaled_progress >= 0.2 and scaled_progress < 0.3
        then '20-29%'
        when scaled_progress >= 0.1 and scaled_progress < 0.2
        then '10-19%'
        else '0-9%'
    end as completion_bucket
from completions
join {{ ref("course_names") }} courses on completions.course_key = courses.course_key
left join
    {{ ref("course_block_names") }} blocks on completions.entity_id = blocks.location
