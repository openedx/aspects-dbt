select 
    '1:2:3 - Course 1a2b3c' as display_name_with_location
from system.one
where 
    {{ section_from_display('display_name_with_location') }}
    != '1:0:0'

union all 

select 
    '1:2:33 - Video 12' as display_name_with_location
from system.one
where 
    {{ section_from_display('display_name_with_location') }}
    != '1:0:0'

union all 

select 
    '1:22:3 - Vertical 345' as display_name_with_location
from system.one
where 
    {{ section_from_display('display_name_with_location') }}
    != '1:0:0'

union all 

select 
    '11:2:3 - Sequential 678' as display_name_with_location
from system.one
where 
    {{ section_from_display('display_name_with_location') }}
    != '11:0:0'
