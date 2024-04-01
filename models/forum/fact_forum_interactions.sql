select
    forum.event_id as event_id,
    forum.emission_time as emission_time,
    forum.org as org,
    forum.course_key as course_key,
    courses.course_name as course_name,
    courses.course_run as course_run,
    forum.object_id as object_id,
    forum.actor_id as actor_id,
    forum.verb_id as verb_id
from {{ ref("forum_events") }} forum
join {{ ref("course_names") }} courses on (forum.course_key = courses.course_key)
