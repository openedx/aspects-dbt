with
    latest_grades as (
        select
            org,
            course_key,
            actor_id,
            splitByString('/', verb_id)[-1] as latest_state,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("grading_events") }}
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/passed',
                'http://adlnet.gov/expapi/verbs/failed'
            )
    ),

    latest_course_grades as (
        select
            org,
            course_key,
            actor_id,
            scaled_score as course_grade,
            row_number() over (
                partition by org, course_key, actor_id order by emission_time desc
            ) as rn
        from {{ ref("fact_grades") }}
        where grade_type = 'course'
    )

select
    lg.org as org,
    lg.course_key as course_key,
    lg.actor_id as actor_id,
    lg.latest_state as latest_state,
    lc.course_grade as course_grade
from latest_grades lg
left join
    latest_course_grades lc
    on lg.org = lc.org
    and lg.course_key = lc.course_key
    and lg.actor_id = lc.actor_id
where lg.rn = 1 and lc.rn = 1
