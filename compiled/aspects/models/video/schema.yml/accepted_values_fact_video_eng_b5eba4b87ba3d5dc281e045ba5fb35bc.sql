
    
    

with all_values as (

    select
        section_subsection_video_engagement as value_field,
        count(*) as n_records

    from `xapi`.`fact_video_engagement`
    group by section_subsection_video_engagement

)

select *
from all_values
where value_field not in (
    'No videos viewed yet','At least one video viewed','All videos viewed'
)


