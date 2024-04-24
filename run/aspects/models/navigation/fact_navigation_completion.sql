

  create view `xapi`.`fact_navigation_completion` 
  
    
    
  as (
    -- number of learners who've viewed all pages in a section/subsection
with
    visited_subsection_pages as (
        select distinct
            date(emission_time) as visited_on,
            org,
            course_key,
            course_name,
            course_run,
            
    concat(
        splitByString(
            ':', splitByString(' - ', block_name_with_location)[1], 1
        )[1],
        ':0:0'
    )
 as section_number,
            
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
            block_id
        from `xapi`.`fact_navigation`
    )

select
    visits.visited_on as visited_on,
    visits.org as org,
    visits.course_key as course_key,
    visits.course_name as course_name,
    visits.course_run as course_run,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    pages.page_count as page_count,
    visits.actor_id as actor_id,
    visits.block_id as block_id,
    users.username as username,
    users.name as name,
    users.email as email
from visited_subsection_pages visits
join
    `xapi`.`int_pages_per_subsection` pages
    on (
        visits.org = pages.org
        and visits.course_key = pages.course_key
        and visits.section_number = pages.section_number
        and visits.subsection_number = pages.subsection_number
    )
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
  )
      
      
                    -- end_of_sql
                    
                    