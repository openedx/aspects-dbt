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
    enrollments.emission_time as emission_time,
    enrollments.org as org,
    courses.course_name as course_name,
    courses.course_run as course_run,
    enrollments.actor_id as actor_id,
    enrollments.enrollment_mode as enrollment_mode,
    enrollments.enrollment_status as enrollment_status
from
    enrollments
    join {{ source('event_sink', 'course_names')}} courses
        on enrollments.course_key = courses.course_key
