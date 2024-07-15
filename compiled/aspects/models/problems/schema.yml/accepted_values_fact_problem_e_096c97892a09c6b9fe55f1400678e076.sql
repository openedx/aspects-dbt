
    
    

with all_values as (

    select
        section_subsection_problem_engagement as value_field,
        count(*) as n_records

    from `xapi`.`fact_problem_engagement`
    group by section_subsection_problem_engagement

)

select *
from all_values
where value_field not in (
    'No problems attempted yet','All problems attempted','At least one problem attempted'
)


