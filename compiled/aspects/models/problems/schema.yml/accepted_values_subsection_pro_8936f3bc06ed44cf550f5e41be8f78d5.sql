
    
    

with all_values as (

    select
        engagement_level as value_field,
        count(*) as n_records

    from `xapi`.`subsection_problem_engagement`
    group by engagement_level

)

select *
from all_values
where value_field not in (
    'No problems attempted yet','All problems attempted','At least one problem attempted'
)


