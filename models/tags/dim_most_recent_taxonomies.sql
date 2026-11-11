{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}
with
    latest as (
        select id, max(time_last_dumped) as last_modified
        from {{ source("event_sink", "taxonomy") }}
        group by id
    )
select taxonomy.id as id, taxonomy.name as name, taxonomy.taxonomy_type as taxonomy_type
from {{ source("event_sink", "taxonomy") }} taxonomy
inner join
    latest
    on latest.id = taxonomy.id
    and taxonomy.time_last_dumped = latest.last_modified
