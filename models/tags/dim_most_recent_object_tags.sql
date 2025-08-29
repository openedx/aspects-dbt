{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
select
    id,
    object_id,
    argMax(taxonomy, time_last_dumped) as taxonomy,
    argMax(_value, time_last_dumped) as _value,
    argMax(_export_id, time_last_dumped) as _export_id,
    argMax(lineage, time_last_dumped) as lineage
from {{ source("event_sink", "object_tag") }}
group by id, object_id
