{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(location)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

with
    final_results as (
        select
            location,
            display_name as block_name,
            toString(_section)
            || ':'
            || toString(_subsection)
            || ':'
            || toString(_unit)
            || ' - '
            || display_name as display_name_with_location,
            JSONExtractInt(xblock_data_json, 'section') as _section,
            JSONExtractInt(xblock_data_json, 'subsection') as _subsection,
            JSONExtractInt(xblock_data_json, 'unit') as _unit,
            JSONExtractBool(xblock_data_json, 'graded') as graded,
            order as course_order,
            course_key,
            dump_id,
            time_last_dumped
        from {{ source("event_sink", "course_blocks") }} course_blocks
        join
            (
                select location, max(time_last_dumped) as max_time_last_dumped
                from {{ source("event_sink", "course_blocks") }}
                group by location
            ) latest_course_blocks
            on course_blocks.location = latest_course_blocks.location
            and course_blocks.time_last_dumped
            = latest_course_blocks.max_time_last_dumped
    )
select
    location, block_name, display_name_with_location, course_order, course_key, graded
from final_results
