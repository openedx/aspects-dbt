

  create view `xapi`.`int_pages_per_subsection` 
  
    
    
  as (
    with
    pages_per_subsection as (
        select
            org,
            course_key,
            section_number,
            subsection_number,
            course_order,
            count(*) as page_count
        from `xapi`.`dim_course_blocks`
        where block_id like '%@vertical+block@%'
        group by org, course_key, section_number, subsection_number, course_order
    )

select
    pps.org as org,
    pps.course_key as course_key,
    pps.section_number as section_number,
    section_blocks.display_name_with_location as section_with_name,
    pps.subsection_number as subsection_number,
    subsection_blocks.display_name_with_location as subsection_with_name,
    pps.course_order as course_order,
    pps.page_count as page_count
from pages_per_subsection pps
left join
    `xapi`.`dim_course_blocks` section_blocks
    on (
        pps.section_number = section_blocks.hierarchy_location
        and pps.org = section_blocks.org
        and pps.course_key = section_blocks.course_key
        and section_blocks.block_id like '%@chapter+block@%'
    )
left join
    `xapi`.`dim_course_blocks` subsection_blocks
    on (
        pps.subsection_number = subsection_blocks.hierarchy_location
        and pps.org = subsection_blocks.org
        and pps.course_key = subsection_blocks.course_key
        and subsection_blocks.block_id like '%@sequential+block@%'
    )
  )
      
      
                    -- end_of_sql
                    
                    