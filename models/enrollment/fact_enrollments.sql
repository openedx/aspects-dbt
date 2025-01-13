select
    enrollments.emission_time as emission_time,
    enrollments.org as org,
    enrollments.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    enrollments.actor_id as actor_id,
    enrollments.enrollment_mode as enrollment_mode,
    enrollments.enrollment_status as enrollment_status,
    users.username as username,
    users.name as name,
    users.email as email
from {{ ref("enrollment_events") }} enrollments
join
    {{ ref("dim_course_names") }} courses on enrollments.course_key = courses.course_key
left outer join
    {{ ref("dim_user_pii") }} users
    on (actor_id like 'mailto:%' and SUBSTRING(actor_id, 8) = users.email)
    or actor_id = toString(users.external_user_id)
