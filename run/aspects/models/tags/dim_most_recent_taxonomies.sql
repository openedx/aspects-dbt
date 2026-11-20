
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_most_recent_taxonomies`
        ("id", "name", "taxonomy_type")
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`taxonomy`
        group by id
    )
select taxonomy.id as id, taxonomy.name as name, taxonomy.taxonomy_type as taxonomy_type
from `event_sink`.`taxonomy` taxonomy
inner join
    latest
    on latest.id = taxonomy.id
    and taxonomy.time_last_dumped = latest.last_modified
  
  
  