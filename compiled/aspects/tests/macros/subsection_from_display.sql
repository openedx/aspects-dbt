select 
    '1:2:3 - Chapter 123' as display_name_with_location
from system.one
where 
    
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', display_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

    != '1:2:0'

union all 

select 
    '1:2:33 - Problem 456' as display_name_with_location
from system.one
where 
    
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', display_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

    != '1:2:0'

union all 

select 
    '1:22:3 - Sequential 789' as display_name_with_location
from system.one
where 
    
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', display_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

    != '1:22:0'

union all 

select 
    '11:2:3 - Vertical 123' as display_name_with_location
from system.one
where 
    
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', display_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

    != '11:2:0'