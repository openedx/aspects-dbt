-- Per learner, how many of each subsection's and section's items of `block_type` they
-- engaged with, as one row per subsection and per section they engaged with at all.
--
-- `engaged` is a query returning (org, course_key, actor_id, block_id) for each item a
-- learner engaged with. A section's total is every item in the section, including
-- subsections the learner never opened, and only items in the course's latest publish
-- count.
--
-- Returns org, course_key, actor_id, content_level, section_subsection_name,
-- section_with_name, block_id, engaged and item_count, plus `status`, built from
-- `labels`: (none, some, all).
{% macro engagement_by_section(engaged, block_type, labels) %}
    with
        items as (
            select org, course_key, block_id, section_number, subsection_number
            from {{ ref("dim_course_blocks") }}
            where block_type = '{{ block_type }}' and in_latest_publish
        ),
        per_subsection as (
            select
                engaged.org as org,
                engaged.course_key as course_key,
                engaged.actor_id as actor_id,
                items.section_number as section_number,
                items.subsection_number as subsection_number,
                count(distinct engaged.block_id) as engaged
            from ({{ engaged }}) engaged
            join
                items
                on engaged.org = items.org
                and engaged.course_key = items.course_key
                and engaged.block_id = items.block_id
            group by org, course_key, actor_id, section_number, subsection_number
        ),
        subsection_items as (
            select *
            from {{ ref("dim_course_subsection_items") }}
            where block_type = '{{ block_type }}'
        ),
        section_items as (
            select
                org,
                course_key,
                section_number,
                any(section_block_id) as section_block_id,
                any(section_with_name) as section_with_name,
                sum(item_count) as item_count
            from subsection_items
            group by org, course_key, section_number
        ),
        counts as (
            select
                per_subsection.org as org,
                per_subsection.course_key as course_key,
                per_subsection.actor_id as actor_id,
                'subsection' as content_level,
                subsection_items.subsection_with_name as section_subsection_name,
                subsection_items.section_with_name as section_with_name,
                subsection_items.subsection_block_id as block_id,
                per_subsection.engaged as engaged,
                subsection_items.item_count as item_count
            from per_subsection
            join
                subsection_items
                on per_subsection.org = subsection_items.org
                and per_subsection.course_key = subsection_items.course_key
                and per_subsection.section_number = subsection_items.section_number
                and per_subsection.subsection_number
                = subsection_items.subsection_number
            union all
            select
                per_subsection.org as org,
                per_subsection.course_key as course_key,
                per_subsection.actor_id as actor_id,
                'section' as content_level,
                any(section_items.section_with_name) as section_subsection_name,
                any(section_items.section_with_name) as section_with_name,
                any(section_items.section_block_id) as block_id,
                sum(per_subsection.engaged) as engaged,
                any(section_items.item_count) as item_count
            from per_subsection
            join
                section_items
                on per_subsection.org = section_items.org
                and per_subsection.course_key = section_items.course_key
                and per_subsection.section_number = section_items.section_number
            group by org, course_key, actor_id, per_subsection.section_number
        )
    select
        *,
        case
            when engaged = 0
            then '{{ labels[0] }}'
            when engaged = item_count
            then '{{ labels[2] }}'
            else '{{ labels[1] }}'
        end as status
    from counts
{% endmacro %}
