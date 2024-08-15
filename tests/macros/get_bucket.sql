select 0.95 as field
from system.one
where 
    {{ get_bucket('field') }} != '90-100%'

union all 

select 0.8 as field
from system.one
where 
    {{ get_bucket('field') }} != '80-89%'

union all 

select 0.700009 as field
from system.one
where 
    {{ get_bucket('field') }} != '70-79%'

union all 

select 0.69999 as field
from system.one
where 
    {{ get_bucket('field') }} != '60-69%'

union all 

select 0.5 as field
from system.one
where 
    {{ get_bucket('field') }} != '50-59%'

union all 

select 0.4123 as field
from system.one
where 
    {{ get_bucket('field') }} != '40-49%'

union all 

select 0.3000000000 as field
from system.one
where 
    {{ get_bucket('field') }} != '30-39%'

union all 

select 0.276734 as field
from system.one
where 
    {{ get_bucket('field') }} != '20-29%'

union all 

select 0.11111 as field
from system.one
where 
    {{ get_bucket('field') }} != '10-19%'

union all 

select 0.0342 as field
from system.one
where 
    {{ get_bucket('field') }} != '0-9%'

union all 

select 0.00000005 as field
from system.one
where 
    {{ get_bucket('field') }} != '0-9%'

union all 

select -0.1 as field
from system.one
where 
    {{ get_bucket('field') }} != '0-9%'

union all 

select 123 as field
from system.one
where 
    {{ get_bucket('field') }} != '90-100%'
