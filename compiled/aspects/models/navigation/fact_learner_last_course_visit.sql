

select org, course_key, actor_id, max(emission_time) as emission_time
from `xapi`.`navigation_events`
group by org, course_key, actor_id