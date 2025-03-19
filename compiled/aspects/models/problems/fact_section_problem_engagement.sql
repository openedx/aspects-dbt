

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
        from `xapi`.`problem_events`
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
            
    concat(
        splitByString(
            ':', splitByString(' - ', problem_name_with_location)[1], 1
        )[1],
        ':0:0'
    )
 as section_number,
            
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', problem_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

            as subsection_number,
            course_order as course_order,
            graded,
            actor_id,
            problem_id
        from fact_problem_responses
    ),
    problems_per_subsection as (
        select * from (
    with
        items_per_subsection as (
            select
                org,
                course_key,
                section_number,
                subsection_number,
                course_order,
                graded,
                count(*) as item_count
            from `xapi`.`dim_course_blocks`
            where block_id like '%@problem+block@%'
            group by
                org, course_key, section_number, subsection_number, course_order, graded
        )

    select
        ips.org as org,
        ips.course_key as course_key,
        ips.section_number as section_number,
        section_blocks.display_name_with_location as section_with_name,
        ips.subsection_number as subsection_number,
        subsection_blocks.display_name_with_location as subsection_with_name,
        ips.course_order as course_order,
        ips.graded as graded,
        ips.item_count as item_count,
        subsection_blocks.block_id as subsection_block_id,
        section_blocks.block_id as section_block_id
    from items_per_subsection ips
    left join
        `xapi`.`dim_course_blocks` section_blocks
        on (
            ips.section_number = section_blocks.hierarchy_location
            and ips.org = section_blocks.org
            and ips.course_key = section_blocks.course_key
            and section_blocks.block_id like '%@chapter+block@%'
        )
    left join
        `xapi`.`dim_course_blocks` subsection_blocks
        on (
            ips.subsection_number = subsection_blocks.hierarchy_location
            and ips.org = subsection_blocks.org
            and ips.course_key = subsection_blocks.course_key
            and subsection_blocks.block_id like '%@sequential+block@%'
        )
)
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