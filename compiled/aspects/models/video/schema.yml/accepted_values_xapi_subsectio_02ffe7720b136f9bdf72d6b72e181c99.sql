

with all_values as (

    select distinct
        engagement_level as value_field

    from xapi.subsection_video_engagement

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'No videos viewed yet','All videos viewed','At least one video viewed'
        )

)

select *
from validation_errors

