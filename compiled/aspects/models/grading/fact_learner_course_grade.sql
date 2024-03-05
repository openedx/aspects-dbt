

with
    ranked_grades as (
        select
            org,
            course_key,
            actor_id,
            scaled_score as course_grade,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from `xapi`.`grading_events`
        where object_id like '%/course/%'
    )

select org, course_key, actor_id, course_grade
from ranked_grades
where rn = 1