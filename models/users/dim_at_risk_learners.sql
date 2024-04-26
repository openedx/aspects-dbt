with
    page_visits as (
        select org, course_key, actor_id, max(emission_time) as last_visited
        from {{ ref("navigation_events") }}
        group by org, course_key, actor_id
    )

select
    learners.org,
    learners.course_key,
    learners.course_name,
    learners.course_run,
    learners.actor_id,
    learners.username,
    learners.name,
    learners.email
from {{ ref("fact_student_status") }} learners
left join page_visits using (org, course_key, actor_id)
where
    -- not yet passing
    approving_state = 'failed'
    -- enrolled in the course
    and enrollment_status = 'registered'
    -- have visited a page other than the course homepage
    and page_visits.actor_id is not null
    -- haven't visited in the last 7 days
    and page_visits.last_visited < subtractDays(now(), 7)
