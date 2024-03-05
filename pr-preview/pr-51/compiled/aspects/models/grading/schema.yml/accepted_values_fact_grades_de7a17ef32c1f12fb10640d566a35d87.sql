
    
    

with all_values as (

    select
        grade_type as value_field,
        count(*) as n_records

    from `xapi`.`fact_grades`
    group by grade_type

)

select *
from all_values
where value_field not in (
    'course','subsection','problem'
)


