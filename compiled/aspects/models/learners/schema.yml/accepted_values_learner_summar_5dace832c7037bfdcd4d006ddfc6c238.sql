
    
    

with all_values as (

    select
        enrollment_status as value_field,
        count(*) as n_records

    from `xapi`.`learner_summary`
    group by enrollment_status

)

select *
from all_values
where value_field not in (
    'registered','unregistered'
)


