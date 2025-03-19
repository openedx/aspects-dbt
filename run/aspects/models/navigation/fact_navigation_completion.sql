

  create view `xapi`.`fact_navigation_completion` 
  
    
    
  as (
    -- number of learners who've viewed all pages in a section/subsection
with
    visited_subsection_pages as (
        select distinct
            date(navigation.emission_time) as visited_on,
            navigation.org as org,
            navigation.course_key as course_key,
            blocks.course_name as course_name,
            blocks.course_run as course_run,
            
    concat(
        splitByString(
            ':', splitByString(' - ', blocks.display_name_with_location)[1], 1
        )[1],
        ':0:0'
    )

            as section_number,
            
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', blocks.display_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

            as subsection_number,
            navigation.actor_id as actor_id,
            navigation.block_id as block_id,
            users.username as username,
            users.name as name,
            users.email as email
        from `xapi`.`navigation_events` navigation
        join
            `xapi`.`dim_course_blocks` blocks
            on (
                navigation.course_key = blocks.course_key
                and navigation.block_id = blocks.block_id
            )
        left outer join
            `xapi`.`dim_user_pii` users
            on (actor_id like 'mailto:%' and SUBSTRING(actor_id, 8) = users.email)
            or actor_id = toString(users.external_user_id)
    ),
    pages_per_subsection as (
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
            where block_id like '%@vertical+block@%'
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
    )
select
    visits.visited_on as visited_on,
    visits.org as org,
    visits.course_key as course_key,
    visits.course_name as course_name,
    visits.course_run as course_run,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    pages.course_order as course_order,
    pages.item_count as page_count,
    visits.actor_id as actor_id,
    visits.block_id as block_id,
    visits.username as username,
    visits.name as name,
    visits.email as email
from visited_subsection_pages visits
join
    pages_per_subsection pages
    on (
        visits.org = pages.org
        and visits.course_key = pages.course_key
        and visits.section_number = pages.section_number
        and visits.subsection_number = pages.subsection_number
    )
    
  )
      
      
                    -- end_of_sql
                    
                    