{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org, course_key, actor_id)",
    )
}}

with
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
            section_subsection_name,
            content_level
        from {{ ref("fact_problem_section_subsection") }} ARRAY
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
            section_subsection_name,
            content_level
    ),
    final_results as (
        select
            problem_engagement.org as org,
            problem_engagement.course_key as course_key,
            problem_engagement.section_subsection_name as section_subsection_name,
            problem_engagement.content_level as content_level,
            problem_engagement.actor_id as actor_id,
            problem_engagement.section_subsection_problem_engagement
            as section_subsection_problem_engagement,
            users.username as username,
            users.name as name,
            users.email as email
        from problem_engagement
        left outer join
            {{ ref("dim_user_pii") }} users
            on (
                problem_engagement.actor_id like 'mailto:%'
                and SUBSTRING(problem_engagement.actor_id, 8) = users.email
            )
            or problem_engagement.actor_id = toString(users.external_user_id)
        where section_subsection_name <> ''
    )
select *
from final_results
