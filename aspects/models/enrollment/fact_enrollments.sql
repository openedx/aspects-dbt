with enrollments as (
    select
        emission_time,
        org,
        course_key,
        actor_id,
        enrollment_mode,
        splitByString('/', verb_id)[-1] as enrollment_status
    from
        {{ source('xapi', 'enrollment_events') }}
)

select
    enrollments.emission_time,
    enrollments.org,
    courses.course_name,
    splitByString('+', courses.course_key)[-1] as run_name,
    enrollments.actor_id,
    enrollments.enrollment_mode,
    enrollments.enrollment_status
from
    enrollments
    join {{ source('event_sink', 'course_names')}} courses
        on enrollments.course_key = courses.course_key
