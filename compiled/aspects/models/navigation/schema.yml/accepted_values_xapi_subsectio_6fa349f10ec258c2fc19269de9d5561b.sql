

with all_values as (

    select distinct
        engagement_level as value_field

    from xapi.subsection_page_engagement

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'No pages viewed yet','All pages viewed','At least one page viewed'
        )

)

select *
from validation_errors

