
    
    

with all_values as (

    select
        content_level as value_field,
        count(*) as n_records

    from `xapi`.`fact_video_engagement`
    group by content_level

)

select *
from all_values
where value_field not in (
    'section','subsection'
)


