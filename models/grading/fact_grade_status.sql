with
    latest_grade_status as (
        select
            org, course_key, object_id, actor_id, max(emission_time) as last_grade_time
        from {{ ref("grading_events") }}
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/passed',
                'http://adlnet.gov/expapi/verbs/failed'
            )
        group by org, course_key, object_id, actor_id
    ),

    latest_grades as (
        select
            org,
            course_key,
            object_id,
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
    )

select
    lg.org,
    lg.course_key,
    lg.object_id,
    lg.actor_id,
    lg.latest_state as latest_grade_state
from latest_grades lg
join
    latest_grade_status lgs
    on lg.org = lgs.org
    and lg.course_key = lgs.course_key
    and lg.object_id = lgs.object_id
    and lg.actor_id = lgs.actor_id
where lg.rn = 1
