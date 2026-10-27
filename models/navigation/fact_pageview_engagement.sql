with
    engagement as (
        {{
            engagement_by_section(
                "select org, course_key, actor_id, block_id from "
                ~ ref("fact_learner_page_visits"),
                "unit",
                (
                    "No pages viewed yet",
                    "At least one page viewed",
                    "All pages viewed",
                ),
            )
        }}
    )
select
    engagement.org as org,
    engagement.course_key as course_key,
    engagement.section_subsection_name as section_subsection_name,
    engagement.content_level as content_level,
    engagement.actor_id as actor_id,
    engagement.status as section_subsection_page_engagement,
    engagement.section_with_name as section_with_name,
    users.username as username,
    users.name as name,
    users.email as email
from engagement
left outer join
    {{ ref("dim_user_pii") }} users
    on (
        engagement.actor_id like 'mailto:%'
        and SUBSTRING(engagement.actor_id, 8) = users.email
    )
    or engagement.actor_id = toString(users.external_user_id)
where engagement.section_subsection_name <> ''
