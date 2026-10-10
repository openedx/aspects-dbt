-- None, some or all videos watched per learner, section and subsection.
{{
    config(
        materialized="view",
        pre_hook=[
            "drop view if exists {{ this.schema }}.fact_video_engagement_mv {{ on_cluster() }}",
        ],
    )
}}

with
    engagement as (
        -- Skips FINAL: watched_seconds never drops back to 0, so an older row can only
        -- repeat a video already counted.
        {{
            engagement_by_section(
                "select org, course_key, actor_id,"
                ~ " splitByString('/xblock/', object_id)[-1] as block_id from "
                ~ ref("fact_video_watches")
                ~ " where watched_seconds > 0",
                "video",
                (
                    "No videos viewed yet",
                    "At least one video viewed",
                    "All videos viewed",
                ),
            )
        }}
    ),
    -- Subsections with videos nobody has watched get a row with a blank learner, and so
    -- do their sections, so they still show up in the charts. A union rather than an
    -- anti-join, so a course filter reaches both sides.
    unwatched as (
        select
            org,
            course_key,
            '' as actor_id,
            tupleElement(level, 1) as content_level,
            tupleElement(level, 2) as section_subsection_name,
            tupleElement(level, 3) as block_id,
            'No videos viewed yet' as status
        from
            (
                select
                    org,
                    course_key,
                    subsection_block_id,
                    any(subsection_with_name) as subsection_with_name,
                    any(section_block_id) as section_block_id,
                    any(section_with_name) as section_with_name
                from
                    (
                        select
                            org,
                            course_key,
                            block_id as subsection_block_id,
                            '' as subsection_with_name,
                            '' as section_block_id,
                            '' as section_with_name,
                            1 as watched
                        from engagement
                        where content_level = 'subsection'
                        union all
                        select
                            org,
                            course_key,
                            subsection_block_id,
                            subsection_with_name,
                            section_block_id,
                            section_with_name,
                            0 as watched
                        from {{ ref("dim_course_subsection_items") }}
                        where block_type = 'video'
                    )
                group by org, course_key, subsection_block_id
                having max(watched) = 0
            ) array
        join
            [
                ('subsection', subsection_with_name, subsection_block_id),
                ('section', section_with_name, section_block_id)
            ] as level
    ),
    final_results as (
        select
            org,
            course_key,
            actor_id,
            content_level,
            section_subsection_name,
            block_id,
            status
        from engagement
        union distinct
        select
            org,
            course_key,
            actor_id,
            content_level,
            section_subsection_name,
            block_id,
            status
        from unwatched
    )
select
    final_results.org as org,
    final_results.course_key as course_key,
    final_results.section_subsection_name as section_subsection_name,
    final_results.content_level as content_level,
    final_results.actor_id as actor_id,
    final_results.status as section_subsection_video_engagement,
    final_results.block_id as block_id,
    users.username as username,
    users.name as name,
    users.email as email
from final_results
left outer join
    {{ ref("dim_user_pii") }} users
    on (
        final_results.actor_id like 'mailto:%'
        and SUBSTRING(final_results.actor_id, 8) = users.email
    )
    or final_results.actor_id = toString(users.external_user_id)
where final_results.section_subsection_name <> ''
