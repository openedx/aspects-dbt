select distinct
    date(problems.emission_time) as attempted_on,
    problems.org as org,
    problems.course_key as course_key,
    blocks.course_run as course_run,
    blocks.section_number as section_number,
    blocks.subsection_number as subsection_number,
    problems.course_order as course_order,
    problems.graded as graded,
    problems.actor_id as actor_id,
    problems.problem_id as problem_id,
    problems.course_name as course_name,
    problems.block_name as problem_name,
    problems.display_name_with_location as problem_name_with_location,
    problems.problem_link as problem_link,
    problems.responses as responses,
    problems.success as success,
    problems.attempts as attempts,
    problems.interaction_type as interaction_type
from {{ ref("problem_events") }} problems
join
    {{ ref("dim_course_blocks") }} blocks
    on (
        problems.course_key = blocks.course_key
        and problems.problem_id = blocks.block_id
    )
where problems.verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
