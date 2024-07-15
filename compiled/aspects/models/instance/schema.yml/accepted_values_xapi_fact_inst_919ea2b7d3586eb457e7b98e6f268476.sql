

with all_values as (

    select distinct
        enrollment_status as value_field

    from xapi.fact_instance_enrollments

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'registered','unregistered'
        )

)

select *
from validation_errors

