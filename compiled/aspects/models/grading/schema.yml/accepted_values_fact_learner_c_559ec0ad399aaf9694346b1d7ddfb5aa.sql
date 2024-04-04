
    
    

with all_values as (

    select
        approving_state as value_field,
        count(*) as n_records

    from `xapi`.`fact_learner_course_status`
    group by approving_state

)

select *
from all_values
where value_field not in (
    'passed','failed'
)


