with
    page_views_by_section as (
        select
            date(emission_time) as emission_date,
            org,
            course_key,
            section_name_with_location,
            course_order,
            actor_id,
            count(*) as total_views
        from {{ ref("fact_navigation") }}
        group by emission_date, org, course_key, section_name_with_location, course_order, actor_id
    ),
    page_views_by_subsection as (
        select
            date(emission_time) as emission_date,
            org,
            course_key,
            subsection_name_with_location,
            course_order,
            actor_id,
            count(*) as total_views
        from {{ ref("fact_navigation") }}
        group by emission_date, org, course_key, subsection_name_with_location, course_order, actor_id
    ),
    page_views as (
        select
            emission_date,
            org,
            course_key,
            'section' as rollup_name,
            section_name_with_location as hierarchy_location,
            course_order as course_order,
            actor_id,
            sum(total_views) as total_views
        from page_views_by_section
        group by
            emission_date, org, course_key, rollup_name, hierarchy_location, course_order, actor_id
        union all
        select
            emission_date,
            org,
            course_key,
            'subsection' as rollup_name,
            subsection_name_with_location as hierarchy_location,
            course_order as course_order,
            actor_id,
            sum(total_views) as total_views
        from page_views_by_subsection
        group by
            emission_date, org, course_key, rollup_name, hierarchy_location, course_order, actor_id
    )

select
    page_views.emission_date as emission_date,
    page_views.org as org,
    page_views.course_key as course_key,
    page_views.rollup_name as rollup_name,
    page_views.hierarchy_location as block_name,
    page_views.course_order as course_order,
    page_views.actor_id as actor_id,
    page_views.total_views as total_views
from page_views
