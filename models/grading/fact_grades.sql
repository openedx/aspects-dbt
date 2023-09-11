with grades as (
    select
        emission_time,
        org,
        course_key,
        case
            when object_id like '%/course/%' then 'course'
            when object_id like '%@sequential+block@%' then 'subsection'
            when object_id like '%@problem+block@%' then 'problem'
        end as grade_type,
        if(
            grade_type = 'course',
            splitByString('/course/', object_id)[-1],
            splitByString('/xblock/', object_id)[-1]
        ) as entity_id,
        actor_id,
        scaled_score
    from
        {{ source('xapi', 'grading_events') }}
)

select
    grades.emission_time as emission_time,
    grades.org as org,
    grades.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    grades.entity_id as entity_id,
    if(blocks.block_name != '', blocks.block_name, courses.course_name) as entity_name,
    grades.grade_type as grade_type,
    grades.actor_id as actor_id,
    cast(grades.scaled_score as Float) as scaled_score,
    case
        when scaled_score > 0.9 then '90-100%'
        when scaled_score > 0.8 then '80-89%'
        when scaled_score > 0.7 then '70-79%'
        else '< 70%'
    end as grade_bucket
from
    grades
    join {{ source('event_sink', 'course_names')}} courses
        on grades.course_key = courses.course_key
    left join {{ source('event_sink', 'course_block_names') }} blocks
        on grades.entity_id = blocks.location
