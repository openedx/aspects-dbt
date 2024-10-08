{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from {{ source("event_sink", "object_tag") }}
        group by id
    ),
    most_recent as (
        select id, object_id, taxonomy, _value, _export_id, lineage
        from {{ source("event_sink", "object_tag") }} ot
        inner join
            latest mrot on mrot.id = ot.id and ot.time_last_dumped = mrot.last_modified
    )
select *
from most_recent
