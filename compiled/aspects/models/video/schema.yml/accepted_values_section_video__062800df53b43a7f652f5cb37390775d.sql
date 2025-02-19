
    
    

with all_values as (

    select
        engagement_level as value_field,
        count(*) as n_records

    from `xapi`.`section_video_engagement`
    group by engagement_level

)

select *
from all_values
where value_field not in (
    'No videos viewed yet','All videos viewed','At least one video viewed'
)


