-- One row per learner and unit they navigated away from, with the latest visit. Kept up
-- to date as events arrive, so engagement reports don't regroup every navigation event.


select org, course_key, actor_id, block_id, emission_time
from `xapi`.`navigation_events`