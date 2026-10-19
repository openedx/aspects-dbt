-- One row per learner and problem they submitted, with the latest submission. Kept up
-- to date as events arrive, so engagement reports don't regroup every problem event.


select org, course_key, actor_id, problem_id, emission_time
from `xapi`.`problem_events`
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'