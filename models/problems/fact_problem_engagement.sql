with
    engagement as (
        {{
            engagement_by_section(
                "select org, course_key, actor_id, problem_id as block_id from "
                ~ ref("fact_learner_problem_attempts"),
                "problem",
                (
                    "No problems attempted yet",
                    "At least one problem attempted",
                    "All problems attempted",
                ),
            )
        }}
    )
select
    engagement.org as org,
    engagement.course_key as course_key,
    engagement.section_subsection_name as section_subsection_name,
    engagement.section_with_name as section_with_name,
    engagement.content_level as content_level,
    engagement.actor_id as actor_id,
    engagement.status as section_subsection_problem_engagement,
    engagement.block_id as block_id,
    users.username as username,
    users.name as name,
    users.email as email
from engagement
left join
    {{ ref("dim_user_pii") }} users
    on (
        engagement.actor_id like 'mailto:%'
        and SUBSTRING(engagement.actor_id, 8) = users.email
    )
    or engagement.actor_id = toString(users.external_user_id)
where engagement.section_subsection_name <> ''
