-- one record per learner, org, course, and day with
-- enrollment status as of 11:59 UTC of that day
--
-- a range of dates is calculated by converting DateTime fields
-- to Unix timestamps, then generating a range of Unix timestamps,
-- creating one record per timestamp (via arrayJoin), and then converting
-- the timestamps back into dates
{{ config(
  post_hook = ["{{apply_admin_rbac(this)}}", "{{apply_instructor_rbac(this)}}"]
) }}


select
  date(fromUnixTimestamp(
    arrayJoin(
      range(
        toUnixTimestamp(window_start_date),
        toUnixTimestamp(window_end_date),
        86400 -- one day, measured in seconds
      )
    )
  )) as enrollment_status_date,
  org,
  course_id,
  actor_id,
  enrollment_mode,
  if(
    verb_id = 'http://adlnet.gov/expapi/verbs/registered',
    'registered',
    'unregistered'
  ) as enrollment_status
from
  {{ ref('int_enrollment_windows') }}
