

  create view `xapi`.`int_problems_per_subsection` 
  
    
    
  as (
    select * from (
    with
        items_per_subsection as (
            select
                org,
                course_key,
                section_number,
                subsection_number,
                graded,
                count(*) as item_count
            from `xapi`.`dim_course_blocks`
            where block_id like '%@problem+block@%'
            group by org, course_key, section_number, subsection_number, graded
        )

    select
        ips.org as org,
        ips.course_key as course_key,
        ips.section_number as section_number,
        section_blocks.display_name_with_location as section_with_name,
        ips.subsection_number as subsection_number,
        subsection_blocks.display_name_with_location as subsection_with_name,
        ips.graded as graded,
        ips.item_count as item_count
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
      
      
                    -- end_of_sql
                    
                    