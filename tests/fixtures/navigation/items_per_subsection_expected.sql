select 
    org,
    course_key,
    section_number,
    section_with_name,
    subsection_number,
    subsection_with_name,
    course_order,
    graded,
    cast(item_count as UInt64) as item_count,
    section_block_id,
    subsection_block_id 
from items_per_subsection_expected