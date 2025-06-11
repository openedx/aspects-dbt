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
    argMax(taxonomy, time_last_dumped) as taxonomy,
    argMax(parent, time_last_dumped) as parent,
    argMax(value, time_last_dumped) as value,
    argMax(external_id, time_last_dumped) as external_id,
    argMax(lineage, time_last_dumped) as lineage
from {{ source("event_sink", "tag") }}
group by id
