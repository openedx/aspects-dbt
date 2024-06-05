{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, subsection_block_id, actor_id)",
    )
}}

with
    responses as (
        select
            emission_time,
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            responses,
            success,
            attempts,
            interaction_type
        from {{ ref("problem_events") }}
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    ),
    fact_problem_responses as (
        select
            responses.emission_time as emission_time,
            responses.org as org,
            responses.course_key as course_key,
            blocks.course_name as course_name,
            responses.problem_id as problem_id,
            blocks.block_name as problem_name,
            blocks.display_name_with_location as problem_name_with_location,
            {{ a_tag("responses.object_id", "blocks.block_name") }} as problem_link,
            blocks.graded as graded,
            blocks.section_block_id as section_block_id,
            blocks.subsection_block_id as subsection_block_id,
            blocks.course_order as course_order,
            responses.actor_id as actor_id,
            responses.responses as responses,
            responses.success as success,
            responses.attempts as attempts,
            responses.interaction_type as interaction_type
        from responses
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                responses.course_key = blocks.course_key
                and responses.problem_id = blocks.block_id
            )
        group by
            -- multi-part questions include an extra record for the response to the
            -- first
            -- part of the question. this group by clause eliminates the duplicate
            -- record
            emission_time,
            org,
            course_key,
            course_name,
            problem_id,
            problem_name,
            problem_name_with_location,
            problem_link,
            section_block_id,
            subsection_block_id,
            actor_id,
            responses,
            success,
            attempts,
            course_order,
            graded,
            interaction_type
    ),
    attempted_subsection_problems as (
        select distinct
            date(emission_time) as attempted_on,
            org,
            course_key,
            section_block_id,
            subsection_block_id,
            course_order as course_order,
            graded,
            actor_id,
            problem_id
        from fact_problem_responses
    ),
    fact_problem_engagement_per_subsection as (
        select
            attempts.org as org,
            attempts.course_key as course_key,
            problems.section_name_with_location as section_name_with_location,
            problems.subsection_name_with_location as subsection_name_with_location,
            problems.item_count as item_count,
            attempts.actor_id as actor_id,
            attempts.problem_id as problem_id,
            problems.subsection_block_id as subsection_block_id
        from attempted_subsection_problems attempts
        join
            {{ ref("int_problems_per_subsection") }} problems
            on attempts.section_block_id = problems.section_block_id
            and attempts.subsection_block_id = problems.subsection_block_id
    ),
    subsection_counts as (
        select
            org,
            course_key,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            item_count,
            count(distinct problem_id) as problems_attempted,
            case
                when problems_attempted = 0
                then 'No problems attempted yet'
                when problems_attempted = item_count
                then 'All problems attempted'
                else 'At least one problem attempted'
            end as engagement_level,
            subsection_block_id
        from fact_problem_engagement_per_subsection
        group by
            org,
            course_key,
            section_name_with_location,
            subsection_name_with_location,
            actor_id,
            item_count,
            subsection_block_id
    )

select org, course_key, actor_id, subsection_block_id, engagement_level
from subsection_counts
