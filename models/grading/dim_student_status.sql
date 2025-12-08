select
    enrollment.org as org,
    enrollment.course_key as course_key,
    enrollment.actor_id as actor_id,
    if(
        empty(state.approving_state), 'failed', state.approving_state
    ) as approving_state,
    enrollment.enrollment_mode as enrollment_mode,
    enrollment.enrollment_status as enrollment_status,
    state.course_grade as course_grade,
    case
        when course_grade >= 0.9
        then '90-100%'
        when course_grade >= 0.8
        then '80-89%'
        when course_grade >= 0.7
        then '70-79%'
        when course_grade >= 0.6
        then '60-69%'
        when course_grade >= 0.5
        then '50-59%'
        when course_grade >= 0.4
        then '40-49%'
        when course_grade >= 0.3
        then '30-39%'
        when course_grade >= 0.2
        then '20-29%'
        when course_grade >= 0.1
        then '10-19%'
        else '0-9%'
    end as grade_bucket,
    enrollment.emission_time as enrolled_at,
    visit.last_navigated as last_navigated,
    GREATEST(visit.last_navigated, enrollment.emission_time) as last_visited
from {{ ref("dim_most_recent_enrollment") }} enrollment
left join
    {{ ref("dim_learner_most_recent_course_state") }} state
    on enrollment.org = state.org
    and enrollment.course_key = state.course_key
    and enrollment.actor_id = state.actor_id
left join
    (
        select org, course_key, actor_id, max(emission_time) as last_navigated
        from {{ ref("navigation_events") }}
        group by org, course_key, actor_id
    ) visit
    on enrollment.org = visit.org
    and enrollment.course_key = visit.course_key
    and enrollment.actor_id = visit.actor_id
