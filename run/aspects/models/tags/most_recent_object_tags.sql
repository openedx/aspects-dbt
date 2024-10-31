create materialized view if not exists `xapi`.`most_recent_object_tags_mv` 
  
  to `xapi`.`most_recent_object_tags`
  as 
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`object_tag`
        group by id
    ),
    most_recent as (
        select id, object_id, taxonomy, _value, _export_id, lineage
        from `event_sink`.`object_tag` ot
        inner join
            latest mrot on mrot.id = ot.id and ot.time_last_dumped = mrot.last_modified
    )
select *
from most_recent