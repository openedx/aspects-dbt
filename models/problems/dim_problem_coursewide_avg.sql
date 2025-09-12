with
    last_response as (
        select
            org,
            course_key,
            problem_link,
            problem_id,
            actor_id,
            course_order,
            graded,
            case when success then attempts else 0 end as success_attempt,
            case when not success then attempts else 0 end as incorrect_attempt
        from {{ ref("dim_learner_last_response") }}
    ),
    coursewide_attempts as (
        select
            org,
            course_key,
            problem_id,
            avg(success_attempt) as avg_correct_attempts,
            avg(incorrect_attempt) as avg_incorrect_attempts,
            countIf(success_attempt > 0) / count(*) as coursewide_percent_correct
        from last_response
        group by org, course_key, problem_id
    )
select
    last_response.org as org,
    last_response.course_key as course_key,
    last_response.course_order as course_order,
    last_response.problem_link as problem_link,
    last_response.graded as graded,
    last_response.actor_id as actor_id,
    coursewide_attempts.avg_correct_attempts as avg_correct_attempts_coursewide,
    coursewide_attempts.avg_incorrect_attempts as avg_incorrect_attempts_coursewide,
    coursewide_attempts.coursewide_percent_correct as coursewide_percent_correct,
    last_response.success_attempt as correct_attempts_by_learner,
    last_response.incorrect_attempt as incorrect_attempts_by_learner,
    CountIf(success_attempt > 0) / count(1) as selected_learner_percent_correct,
    CountIf(incorrect_attempt > 0) / count(1) as selected_learner_percent_incorrect,
    users.username as username,
    users.email as email,
    users.name as name
from last_response
join
    coursewide_attempts
    on last_response.org = coursewide_attempts.org
    and last_response.course_key = coursewide_attempts.course_key
    and last_response.problem_id = coursewide_attempts.problem_id
left outer join
    {{ ref("dim_user_pii") }} users
    on (
        last_response.actor_id like 'mailto:%'
        and SUBSTRING(last_response.actor_id, 8) = users.email
    )
    or last_response.actor_id = toString(users.external_user_id)
group by
    org,
    course_key,
    course_order,
    problem_link,
    graded,
    actor_id,
    avg_correct_attempts_coursewide,
    avg_incorrect_attempts_coursewide,
    coursewide_percent_correct,
    correct_attempts_by_learner,
    incorrect_attempts_by_learner,
    username,
    email,
    name
