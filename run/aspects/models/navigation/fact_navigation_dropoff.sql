

  create view `xapi`.`fact_navigation_dropoff` 
  
    
    
  as (
    with
    blocks as (
        select org, course_key, display_name_with_location, hierarchy_location
        from `xapi`.`dim_course_blocks`
        where block_id like '%@chapter+block@%' or block_id like '%@sequential+block@%'
    ),
    page_views_by_section as (
        -- section: x:0:0
        -- take just the first number from the hierarchy location
        select
            date(emission_time) as emission_date,
            org,
            course_key,
            
    concat(
        splitByString(
            ':', splitByString(' - ', block_name_with_location)[1], 1
        )[1],
        ':0:0'
    )
 as section_number,
            actor_id,
            count(*) as total_views
        from `xapi`.`fact_navigation`
        group by emission_date, org, course_key, section_number, actor_id
    ),
    page_views_by_subsection as (
        -- subsection: x:y:0
        -- take the first two numbers from the hierarchy location
        select
            date(emission_time) as emission_date,
            org,
            course_key,
            
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', block_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

            as subsection_number,
            actor_id,
            count(*) as total_views
        from `xapi`.`fact_navigation`
        group by emission_date, org, course_key, subsection_number, actor_id
    ),
    page_views as (
        select
            emission_date,
            org,
            course_key,
            'section' as rollup_name,
            section_number as hierarchy_location,
            actor_id,
            sum(total_views) as total_views
        from page_views_by_section
        group by
            emission_date, org, course_key, rollup_name, hierarchy_location, actor_id
        union all
        select
            emission_date,
            org,
            course_key,
            'subsection' as rollup_name,
            subsection_number as hierarchy_location,
            actor_id,
            sum(total_views) as total_views
        from page_views_by_subsection
        group by
            emission_date, org, course_key, rollup_name, hierarchy_location, actor_id
    )

select
    page_views.emission_date as emission_date,
    page_views.org as org,
    page_views.course_key as course_key,
    page_views.rollup_name as rollup_name,
    blocks.display_name_with_location as block_name,
    page_views.actor_id as actor_id,
    page_views.total_views as total_views,
    users.username as username,
    users.name as name,
    users.email as email
from page_views
join
    blocks
    on (
        page_views.org = blocks.org
        and page_views.course_key = blocks.course_key
        and page_views.hierarchy_location = blocks.hierarchy_location
    )
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
  )
      
      
                    -- end_of_sql
                    
                    