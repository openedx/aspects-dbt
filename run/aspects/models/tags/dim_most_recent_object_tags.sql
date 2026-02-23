
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_most_recent_object_tags`
        ("id", "object_id", "taxonomy", "_value", "_export_id", "lineage")
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`object_tag`
        group by id
    )
select
    object_tag.id as id,
    object_tag.object_id as object_id,
    object_tag.taxonomy as taxonomy,
    object_tag._value as _value,
    object_tag._export_id as _export_id,
    object_tag.lineage as lineage
from `event_sink`.`object_tag` object_tag
inner join
    latest
    on latest.id = object_tag.id
    and object_tag.time_last_dumped = latest.last_modified
  
  
  