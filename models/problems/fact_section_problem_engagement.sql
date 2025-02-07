{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key)",
        order_by="(org, course_key, section_block_id, actor_id)",
    )
}}

with
    fact_problem_responses as (
        select
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
    problems_per_subsection as (
        select * from ({{ items_per_subsection("%@problem+block@%") }})
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
            problems_per_subsection problems
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

select org, course_key, actor_id, section_block_id, engagement_level
from section_counts
