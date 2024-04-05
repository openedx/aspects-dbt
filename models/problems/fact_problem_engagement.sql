with
    attempted_subsection_problems as (
        select distinct
            date(emission_time) as attempted_on,
            org,
            course_key,
            course_run,
            {{ section_from_display("problem_name_with_location") }} as section_number,
            {{ subsection_from_display("problem_name_with_location") }}
            as subsection_number,
            graded,
            actor_id,
            problem_id
        from {{ ref("fact_problem_responses") }}
    )

select
    attempts.attempted_on,
    attempts.org,
    attempts.course_key,
    attempts.course_run,
    problems.section_with_name,
    problems.subsection_with_name,
    problems.item_count,
    attempts.actor_id,
    attempts.problem_id,
    attempts.graded
from attempted_subsection_problems attempts
join
    {{ ref("int_problems_per_subsection") }} problems
    on (
        attempts.org = problems.org
        and attempts.course_key = problems.course_key
        and attempts.section_number = problems.section_number
        and attempts.subsection_number = problems.subsection_number
    )
