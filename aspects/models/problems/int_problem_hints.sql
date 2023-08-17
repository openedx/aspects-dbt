with hints as (
    select
        emission_time,
        org,
        course_key,
        {{ get_problem_id('object_id') }} as problem_id,
        actor_id,
        case
            when object_id like '%/hint%' then 'hint'
            when object_id like '%/answer%' then 'answer'
            else 'N/A'
        end as help_type
    from
        {{ source('xapi', 'problem_events') }}
    where
        verb_id = 'http://adlnet.gov/expapi/verbs/asked'
)

select
    hints.emission_time as emission_time,
    hints.org as org,
    hints.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    hints.problem_id as problem_id,
    blocks.block_name as problem_name,
    hints.actor_id as actor_id,
    hints.help_type as help_type
from
    hints
    join {{ source('event_sink', 'course_names')}} courses
         on hints.course_key = courses.course_key
    join {{ source('event_sink', 'course_block_names')}} blocks
         on hints.problem_id = blocks.location
