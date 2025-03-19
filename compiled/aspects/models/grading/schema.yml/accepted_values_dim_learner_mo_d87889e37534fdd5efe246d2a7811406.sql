
    
    

with all_values as (

    select
        approving_state as value_field,
        count(*) as n_records

    from `xapi`.`dim_learner_most_recent_course_state`
    group by approving_state

)

select *
from all_values
where value_field not in (
    'passed','failed'
)


