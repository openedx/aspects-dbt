
    
    

with all_values as (

    select
        state as value_field,
        count(*) as n_records

    from `xapi`.`fact_grade_status`
    group by state

)

select *
from all_values
where value_field not in (
    'passed','failed'
)


