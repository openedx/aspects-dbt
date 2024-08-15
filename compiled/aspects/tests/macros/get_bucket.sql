select 0.95 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '90-100%'

union all 

select 0.8 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '80-89%'

union all 

select 0.700009 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '70-79%'

union all 

select 0.69999 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '60-69%'

union all 

select 0.5 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '50-59%'

union all 

select 0.4123 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '40-49%'

union all 

select 0.3000000000 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '30-39%'

union all 

select 0.276734 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '20-29%'

union all 

select 0.11111 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '10-19%'

union all 

select 0.0342 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '0-9%'

union all 

select 0.00000005 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '0-9%'

union all 

select -0.1 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '0-9%'

union all 

select 123 as field
from system.one
where 
    case
        when field >= 0.9
        then '90-100%'
        when field >= 0.8 and field < 0.9
        then '80-89%'
        when field >= 0.7 and field < 0.8
        then '70-79%'
        when field >= 0.6 and field < 0.7
        then '60-69%'
        when field >= 0.5 and field < 0.6
        then '50-59%'
        when field >= 0.4 and field < 0.5
        then '40-49%'
        when field >= 0.3 and field < 0.4
        then '30-39%'
        when field >= 0.2 and field < 0.3
        then '20-29%'
        when field >= 0.1 and field < 0.2
        then '10-19%'
        else '0-9%'
    end != '90-100%'