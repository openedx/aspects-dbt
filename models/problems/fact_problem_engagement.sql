{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, block_id, actor_id)",
        primary_key="(org, course_key, block_id, actor_id)",
    )
}}

with
    get_problem_data as (
        select
            problems.org as org,
            problems.course_key as course_key,
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
            {{ ref("dim_items_per_subsection") }} items
            on (
                problems.org = items.org
                and problems.course_key = items.course_key
                and blocks.section_number = items.section_number
                and blocks.subsection_number = items.subsection_number
            )
        where
            problems.verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
            and items.block_type = 'problem+block'
    ),
    section_subsection as (
        select
            org,
            course_key,
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
    org,
    course_key,
    section_subsection_name,
    section_with_name,
    content_level,
    actor_id,
    section_subsection_problem_engagement,
    block_id
from problem_engagement
where section_subsection_name <> ''
