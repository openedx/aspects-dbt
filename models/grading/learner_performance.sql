with
    full_responses as (
        select
            events.org as org,
            events.course_key as course_key,
            events.object_id as object_id,
            events.actor_id as actor_id,
            events.success as success,
            events.attempts as attempts,
            blocks.course_name as course_name,
            blocks.course_run as course_run
        from {{ ref('dim_student_status') }} student
        join {{ ref('dim_learner_response_attempt') }} attempt on (
            student.org = attempt.org
            and student.course_key = attempt.course_key
            and student.course_run = attempt.course_run
            and student.actor_id = attempt.actor_id
            and student.course_name = attempt.course_name
        )
        join
            {{ ref('problem_events') }} events on (
                attempt.org = events.org
                and attempt.course_key = events.course_key
                and attempt.problem_id = events.problem_id
                and attempt.actor_id = events.actor_id
                and attempt.emission_time = events.emission_time
            )
        join
            {{ ref('dim_course_blocks') }} blocks
            on (
                course_key = blocks.course_key
                and problem_id = blocks.block_id
            )
    )
select
    org,
    course_key,
    course_name,
    course_run,
    actor_id,
    success,
    attempts,
    course_grade,
    approving_state
from {{ ref('dim_student_status') }}
left join full_responses
using org, course_key, course_run, actor_id, course_name
