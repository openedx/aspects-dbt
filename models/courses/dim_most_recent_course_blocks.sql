{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(location)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

select
    course_blocks.location as location,
    course_blocks.display_name as block_name,
    toString(section)
    || ':'
    || toString(subsection)
    || ':'
    || toString(unit)
    || ' - '
    || course_blocks.display_name as display_name_with_location,
    JSONExtractInt(course_blocks.xblock_data_json, 'section') as section,
    JSONExtractInt(course_blocks.xblock_data_json, 'subsection') as subsection,
    JSONExtractInt(course_blocks.xblock_data_json, 'unit') as unit,
    JSONExtractBool(course_blocks.xblock_data_json, 'graded') as graded,
    JSONExtractFloat(
        course_blocks.xblock_data_json, 'video_start_time'
    ) as video_start_time,
    JSONExtractFloat(
        course_blocks.xblock_data_json, 'video_end_time'
    ) as video_end_time,
    course_blocks.order as course_order,
    course_blocks.course_key as course_key,
    course_blocks.dump_id as dump_id,
    course_blocks.time_last_dumped as time_last_dumped
from {{ source("event_sink", "course_blocks") }} course_blocks
join
    (
        select location, max(time_last_dumped) as max_time_last_dumped
        from {{ source("event_sink", "course_blocks") }}
        group by location
    ) latest_course_blocks
    on course_blocks.location = latest_course_blocks.location
    and course_blocks.time_last_dumped = latest_course_blocks.max_time_last_dumped
