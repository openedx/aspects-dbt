
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_most_recent_course_blocks`
        ("location", "block_name", "display_name_with_location", "section", "subsection", "unit", "graded", "course_order", "course_key", "dump_id", "time_last_dumped")

select
    location,
    display_name as block_name,
    toString(section)
    || ':'
    || toString(subsection)
    || ':'
    || toString(unit)
    || ' - '
    || display_name as display_name_with_location,
    JSONExtractInt(xblock_data_json, 'section') as section,
    JSONExtractInt(xblock_data_json, 'subsection') as subsection,
    JSONExtractInt(xblock_data_json, 'unit') as unit,
    JSONExtractBool(xblock_data_json, 'graded') as graded,
    order as course_order,
    course_key,
    dump_id,
    time_last_dumped
from `event_sink`.`course_blocks` course_blocks
join
    (
        select location, max(time_last_dumped) as max_time_last_dumped
        from `event_sink`.`course_blocks`
        group by location
    ) latest_course_blocks
    on course_blocks.location = latest_course_blocks.location
    and course_blocks.time_last_dumped = latest_course_blocks.max_time_last_dumped
  
  
  