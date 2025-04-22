with
    get_problem_data as (
        select
            attempts.org as org,
            attempts.course_key as course_key,
            attempts.course_order as course_order,
            attempts.actor_id as actor_id,
            items.original_block_id as original_block_id,
            attempts.problem_id as problem_id,
            items.section_block_id as section_block_id,
            items.subsection_block_id as subsection_block_id,
            items.section_with_name as section_with_name,
            items.subsection_with_name as subsection_with_name
        from {{ ref("items_per_subsection") }} items
        join
            {{ ref("fact_problem_events_evaluated") }} attempts
            on (
                attempts.org = items.org
                and attempts.course_key = items.course_key
                and attempts.section_number = items.section_number
                and attempts.subsection_number = items.subsection_number
            )
        where items.original_block_id like '%@problem+block@%'
    )
select
    org,
    course_key,
    course_order,
    actor_id,
    'section' as section_content_level,
    'subsection' as subsection_content_level,
    count(original_block_id) as item_count,
    count(distinct problem_id) as problems_attempted,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name,
from get_problem_data
group by
    org,
    course_key,
    course_order,
    actor_id,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
