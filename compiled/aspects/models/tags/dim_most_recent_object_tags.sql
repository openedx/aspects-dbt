
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from `event_sink`.`object_tag`
        group by id
    )
select id, object_id, taxonomy, _value, _export_id, lineage
from `event_sink`.`object_tag` ot
inner join latest mrot on mrot.id = ot.id and ot.time_last_dumped = mrot.last_modified