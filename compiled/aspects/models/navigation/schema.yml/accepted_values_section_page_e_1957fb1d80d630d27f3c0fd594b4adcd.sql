
    
    

with all_values as (

    select
        engagement_level as value_field,
        count(*) as n_records

    from `xapi`.`section_page_engagement`
    group by engagement_level

)

select *
from all_values
where value_field not in (
    'No problems attempted yet','All problems attempted','At least one problem attempted'
)


