with enrollments_ranked as (
  select
    actor_id,
    object_id,
    course_id,
    org,
    event_type,
    enrollment_mode,
    emission_time as window_start_at,
    anyOrNull(emission_time)
      over (partition by org, course_id, actor_id order by emission_time asc rows between 1 following and 1 following)
      as window_end_at,
    rank() over (partition by date(emission_time), org, course_id, actor_id order by emission_time desc) as rank
  from
    {{ source('xapi', 'enrollment_events') }}
)

select
  actor_id,
  object_id,
  course_id,
  org,
  event_type,
  enrollment_mode,
  date_trunc('day', window_start_at) as window_start_date,
  date_trunc('day', coalesce(window_end_at, now())) as window_end_date,
  rank
from
  enrollments_ranked
where
  rank = 1
