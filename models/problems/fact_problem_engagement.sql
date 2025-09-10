with
    get_problem_data as (
        select
            problems.org as org,
            problems.course_key as course_key,
            items.subsection_course_order as course_order,
            problems.actor_id as actor_id,
            items.item_count as item_count,
            problems.problem_id as problem_id,
            items.section_block_id as section_block_id,
            items.subsection_block_id as subsection_block_id,
            items.section_with_name as section_with_name,
            items.subsection_with_name as subsection_with_name
        from {{ ref("problem_events") }} problems
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                problems.course_key = blocks.course_key
                and problems.problem_id = blocks.block_id
            )
        join
            (select * from ({{ items_per_subsection("%@problem+block@%") }})) items
            on (
                problems.org = items.org
                and problems.course_key = items.course_key
                and blocks.section_number = items.section_number
                and blocks.subsection_number = items.subsection_number
            )
        where problems.verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    ),
    section_subsection as (
        select
            org,
            course_key,
            course_order,
            actor_id,
            'section' as section_content_level,
            'subsection' as subsection_content_level,
            item_count,
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
            item_count,
            section_block_id,
            subsection_block_id,
            section_with_name,
            subsection_with_name
    ),
    problem_engagement as (
        select
            org,
            course_key,
            actor_id,
            case
                when problems_attempted = 0
                then 'No problems attempted yet'
                when problems_attempted = item_count
                then 'All problems attempted'
                else 'At least one problem attempted'
            end as section_subsection_problem_engagement,
            block_id,
            section_with_name,
            section_subsection_name,
            content_level
        from section_subsection ARRAY
        join
            arrayConcat([subsection_block_id], [section_block_id]) as block_id,
            arrayConcat(
                [subsection_with_name], [section_with_name]
            ) as section_subsection_name,
            arrayConcat(
                [subsection_content_level], [section_content_level]
            ) as content_level
        group by
            org,
            course_key,
            actor_id,
            section_subsection_problem_engagement,
            block_id,
            section_with_name,
            section_subsection_name,
            content_level
    )
select
    problem_engagement.org as org,
    problem_engagement.course_key as course_key,
    problem_engagement.section_subsection_name as section_subsection_name,
    problem_engagement.section_with_name as section_with_name,
    problem_engagement.content_level as content_level,
    problem_engagement.actor_id as actor_id,
    problem_engagement.section_subsection_problem_engagement
    as section_subsection_problem_engagement,
    problem_engagement.block_id as block_id,
    users.username as username,
    users.name as name,
    users.email as email
from problem_engagement
left join
    {{ ref("dim_user_pii") }} users
    on (
        problem_engagement.actor_id like 'mailto:%'
        and SUBSTRING(problem_engagement.actor_id, 8) = users.email
    )
    or problem_engagement.actor_id = toString(users.external_user_id)
where section_subsection_name <> ''
