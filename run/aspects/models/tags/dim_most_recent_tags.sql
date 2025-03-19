
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_most_recent_tags`
        ("id", "taxonomy", "parent", "value", "external_id", "lineage")
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`tag`
        group by id
    )
select id, taxonomy, parent, value, external_id, lineage
from `event_sink`.`tag` ot
inner join latest mrot on mrot.id = ot.id and ot.time_last_dumped = mrot.last_modified
  
  
  