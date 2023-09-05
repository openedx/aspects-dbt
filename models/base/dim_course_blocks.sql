select
    courses.org as org,
    courses.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    blocks.location as block_id,
    blocks.block_name as block_name
from
    {{ source('event_sink', 'course_block_names') }} blocks
    join {{ source('event_sink', 'course_names')}} courses
        on blocks.course_key = courses.course_key
settings join_algorithm='direct'
