{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
    )
}}

with
    fact_problem_responses as (
        select distinct
            emission_time as emission_time,
            org as org,
            course_key as course_key,
            course_name as course_name,
            problem_id as problem_id,
            block_name as problem_name,
            display_name_with_location as problem_name_with_location,
            problem_link,
            graded as graded,
            course_order as course_order,
            actor_id as actor_id,
            responses as responses,
            success as success,
            attempts as attempts,
            interaction_type as interaction_type
        from {{ ref("problem_events") }}
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    ),
    attempted_subsection_problems as (
        select distinct
            date(problems.emission_time) as attempted_on,
            problems.org,
            problems.course_key,
            blocks.course_run,
            blocks.section_number as section_number,
            blocks.subsection_number as subsection_number,
            problems.course_order as course_order,
            problems.graded,
            problems.actor_id,
            problems.problem_id
        from fact_problem_responses problems
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                plays.course_key = blocks.course_key
                and plays.video_id = blocks.block_id
            )
    ),
    problems as (
        select
            attempts.org,
            attempts.course_key,
            attempts.section_number,
            attempts.subsection_number,
            problems.section_with_name,
            problems.subsection_with_name,
            attempts.course_order,
            count(original_block_id) as item_count,
            count(distinct problem_id) as problems_attempted,
            attempts.section_block_id,
            'section' as section_content_level,
            'subsection' as subsection_content_level
        from {{ ref("items_per_subsection") }} problems
        join
            attempted_subsection_problems attempts
            on (
                attempts.org = problems.org
                and attempts.course_key = problems.course_key
                and attempts.section_number = problems.section_number
                and attempts.subsection_number = problems.subsection_number
            )
        where original_block_id like '%@problem+block@%'
        group by
            section_with_name,
            subsection_with_name,
            course_order,
            org,
            course_key,
            section_number,
            subsection_number,
            section_block_id
    ),
    problem_engagement as (
        select
            org,
            course_key,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            case
                when problems_attempted = 0
                then 'No problems attempted yet'
                when problems_attempted = item_count
                then 'All problems attempted'
                else 'At least one problem attempted'
            end as section_subsection_problem_engagement,
            block_id,
            section_subsection_name,
            content_level
        from problems ARRAY
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
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            section_block_id
    ),
    final_results as (
        select
            pe.org as org,
            pe.course_key as course_key,
            pe.course_run as course_run,
            pe.section_subsection_name as section_subsection_name,
            pe.content_level as content_level,
            pe.actor_id as actor_id,
            pe.section_subsection_problem_engagement
            as section_subsection_problem_engagement,
            users.username as username,
            users.name as name,
            users.email as email
        from problem_engagement pe
        left outer join
            {{ DBT_PROFILE_TARGET_DATABASE }}.dim_user_pii users
            on (pe.actor_id like 'mailto:%' and SUBSTRING(pe.actor_id, 8) = users.email)
            or pe.actor_id = toString(users.external_user_id)
    )
select *
from final_results
