with
    latest_emission_time as (
        select course_key, actor_id, MAX(emission_time) as last_visited
        from `xapi`.`fact_navigation`
        group by course_key, actor_id
    ),
    enrollment_status as (
        select course_key, actor_id, MAX(emission_time) as max_emission_time
        from `xapi`.`fact_enrollment_status`
        group by course_key, actor_id
    )
select
    fss.org as org,
    fss.course_key as course_key,
    fss.actor_id as actor_id,
    fss.course_name as course_name,
    fss.course_run as course_run,
    fss.approving_state as approving_state,
    fss.enrollment_mode as enrollment_mode,
    fss.enrollment_status as enrollment_status,
    fss.course_grade as course_grade,
    fss.grade_bucket as grade_bucket,
    fss.username as username,
    fss.name as name,
    fss.email as email,
    fes.max_emission_time as emission_time,
    let.last_visited as last_visited
from `xapi`.`fact_student_status` fss
left join
    enrollment_status fes
    on fss.course_key = fes.course_key
    and fss.actor_id = fes.actor_id
left join
    latest_emission_time let
    on fss.course_key = let.course_key
    and fss.actor_id = let.actor_id