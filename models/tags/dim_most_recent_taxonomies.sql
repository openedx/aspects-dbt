{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
select id, argMax(name, time_last_dumped) as name
from {{ source("event_sink", "taxonomy") }}
group by id
