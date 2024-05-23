{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key)",
        order_by="(org, course_key, section_block_id, actor_id)",
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
            course_order as course_order,
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
            {{ section_from_display("problem_name_with_location") }} as section_number,
            {{ subsection_from_display("problem_name_with_location") }}
            as subsection_number,
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
            problems.section_with_name as section_with_name,
            problems.subsection_with_name as subsection_with_name,
            problems.item_count as item_count,
            attempts.actor_id as actor_id,
            attempts.problem_id as problem_id,
            problems.section_block_id as section_block_id
        from attempted_subsection_problems attempts
        join
            {{ ref("int_problems_per_subsection") }} problems
            on (
                attempts.org = problems.org
                and attempts.course_key = problems.course_key
                and attempts.section_number = problems.section_number
                and attempts.subsection_number = problems.subsection_number
            )
    ),
    subsection_counts as (
        select
            org,
            course_key,
            section_with_name,
            subsection_with_name,
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
            section_block_id
        from fact_problem_engagement_per_subsection
        group by
            org,
            course_key,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            section_block_id
    ),
    section_counts as (
        select
            org,
            course_key,
            actor_id,
            sum(item_count) as item_count,
            sum(problems_attempted) as problems_attempted,
            case
                when problems_attempted = 0
                then 'No problems attempted yet'
                when problems_attempted = item_count
                then 'All problems attempted'
                else 'At least one problem attempted'
            end as engagement_level,
            section_block_id
        from subsection_counts
        group by org, course_key, section_block_id, actor_id
    )

select org, course_key, actor_id as actor_id, section_block_id, engagement_level
from section_counts
