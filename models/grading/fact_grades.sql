with
    grades as (
        select
            emission_time,
            org,
            course_key,
            case
                when object_id like '%/course/%'
                then 'course'
                when object_id like '%@sequential+block@%'
                then 'subsection'
                when object_id like '%@problem+block@%'
                then 'problem'
            end as grade_type,
            if(
                grade_type = 'course',
                splitByString('/course/', object_id)[-1],
                splitByString('/xblock/', object_id)[-1]
            ) as entity_id,
            actor_id,
            scaled_score
        from {{ ref("grading_events") }}
        where
            verb_id in (
                'http://id.tincanapi.com/verb/earned',
                'https://w3id.org/xapi/acrossx/verbs/evaluated'
            )
    )

select
    grades.emission_time as emission_time,
    grades.org as org,
    grades.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    grades.entity_id as entity_id,
    if(blocks.block_name != '', blocks.block_name, courses.course_name) as entity_name,
    if(
        blocks.block_name != '', blocks.display_name_with_location, null
    ) as entity_name_with_location,
    grades.grade_type as grade_type,
    grades.actor_id as actor_id,
    grades.scaled_score as scaled_score,
    {{ get_bucket("scaled_score") }} as grade_bucket,
    username,
    name,
    email
from grades
join {{ ref("course_names") }} courses on grades.course_key = courses.course_key
left join {{ ref("course_block_names") }} blocks on grades.entity_id = blocks.location
left join {{ ref("dim_user_pii") }} users on toUUID(actor_id) = users.external_user_id
