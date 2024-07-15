

with all_values as (

    select distinct
        engagement_level as value_field

    from xapi.subsection_problem_engagement

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'No problems attempted yet','All problems attempted','At least one problem attempted'
        )

)

select *
from validation_errors

