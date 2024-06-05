with
    attempted_subsection_problems as (
        select distinct
            date(emission_time) as attempted_on,
            org,
            course_key,
            section_block_id,
            subsection_block_id,
            course_run,
            course_order as course_order,
            graded,
            actor_id,
            problem_id
        from {{ ref("fact_problem_responses") }}
    )

select
    attempts.org as org,
    attempts.course_key as course_key,
    attempts.course_run as course_run,
    attempts.section_block_id as section_block_id,
    attempts.subsection_block_id as subsection_block_id,
    problems.section_name_with_location as section_name_with_location,
    problems.subsection_name_with_location as subsection_name_with_location,
    problems.item_count as item_count,
    attempts.actor_id as actor_id,
    attempts.problem_id as problem_id
from attempted_subsection_problems attempts
join
    {{ ref("int_problems_per_subsection") }} problems
    on (
        attempts.org = problems.org
        and attempts.course_key = problems.course_key
        and attempts.section_block_id = problems.section_block_id
        and attempts.subsection_block_id = problems.subsection_block_id
    )
