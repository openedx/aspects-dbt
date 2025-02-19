
  
    
  
    
    
    
        
         


        insert into `xapi`.`most_recent_taxonomies`
        ("id", "name")
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`taxonomy`
        group by id
    ),
    most_recent as (
        select id, name
        from `event_sink`.`taxonomy` ot
        inner join
            latest mrot on mrot.id = ot.id and ot.time_last_dumped = mrot.last_modified
    )
select *
from most_recent
  
  
  