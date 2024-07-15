
    
    

with all_values as (

    select
        rollup_name as value_field,
        count(*) as n_records

    from `xapi`.`fact_navigation_dropoff`
    group by rollup_name

)

select *
from all_values
where value_field not in (
    'section','subsection'
)


