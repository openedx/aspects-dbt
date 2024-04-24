select
    forum.event_id as event_id,
    forum.emission_time as emission_time,
    forum.org as org,
    forum.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    forum.object_id as object_id,
    forum.actor_id as actor_id,
    forum.verb_id as verb_id,
    users.username as username,
    users.name as name,
    users.email as email
from `xapi`.`forum_events` forum
join `xapi`.`course_names` courses on (forum.course_key = courses.course_key)
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id