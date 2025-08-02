
  
    
  
    
    
    
        
         


        insert into `xapi`.`fact_subsection_page_engagement`
        ("org", "course_key", "actor_id", "subsection_block_id", "engagement_level")

with
    fact_navigation as (
        select
            navigation.emission_time as emission_time,
            navigation.org as org,
            navigation.course_key as course_key,
            navigation.actor_id as actor_id,
            navigation.block_id as block_id,
            blocks.display_name_with_location as block_name_with_location
        from `xapi`.`navigation_events` navigation
        join
            `xapi`.`dim_course_blocks` blocks
            on (
                navigation.course_key = blocks.course_key
                and navigation.block_id = blocks.block_id
            )
    ),
    visited_subsection_pages as (
        select distinct
            date(emission_time) as visited_on,
            org,
            course_key,
            
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
        from fact_navigation
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
                min(course_order) as course_order,
                graded,
                count(*) as item_count
            from `xapi`.`dim_course_blocks`
            where block_id like '%@vertical+block@%'
            group by org, course_key, section_number, subsection_number, graded
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
        section_blocks.block_id as section_block_id,
        section_blocks.course_order as section_course_order,
        subsection_blocks.course_order as subsection_course_order
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
    fact_navigation_completion as (
        select
            visits.visited_on as visited_on,
            visits.org as org,
            visits.course_key as course_key,
            pages.section_with_name as section_with_name,
            pages.subsection_with_name as subsection_with_name,
            pages.item_count as item_count,
            visits.actor_id as actor_id,
            visits.block_id as block_id,
            pages.subsection_block_id as subsection_block_id
        from visited_subsection_pages visits
        join
            pages_per_subsection pages
            on (
                visits.org = pages.org
                and visits.course_key = pages.course_key
                and visits.section_number = pages.section_number
                and visits.subsection_number = pages.subsection_number
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
            count(distinct block_id) as pages_visited,
            case
                when pages_visited = 0
                then 'No pages viewed yet'
                when pages_visited = item_count
                then 'All pages viewed'
                else 'At least one page viewed'
            end as engagement_level,
            subsection_block_id
        from fact_navigation_completion
        group by
            org,
            course_key,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            subsection_block_id
    )

select org, course_key, actor_id, subsection_block_id, engagement_level
from subsection_counts
  
  
  