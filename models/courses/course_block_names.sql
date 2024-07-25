{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("location", "String"),
            ("block_name", "String"),
            ("course_key", "String"),
            ("graded", "Bool"),
            ("course_order", "Int32"),
            ("display_name_with_location", "String"),
        ],
        primary_key="location",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime="120",
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

select
    course_blocks.location as location,
    display_name as block_name,
    course_key,
    JSONExtractBool(xblock_data_json, 'graded') as graded,
    order as course_order,
    toString(JSONExtractInt(xblock_data_json, 'section'))
    || ':'
    || toString(JSONExtractInt(xblock_data_json, 'subsection'))
    || ':'
    || toString(JSONExtractInt(xblock_data_json, 'unit'))
    || ' - '
    || display_name as display_name_with_location
from {{ source("event_sink", "course_blocks") }} course_blocks
join
    (
        select location, max(time_last_dumped) as max_time_last_dumped
        from {{ source("event_sink", "course_blocks") }}
        group by location
    ) latest_course_blocks
    on course_blocks.location = latest_course_blocks.location
    and course_blocks.time_last_dumped = latest_course_blocks.max_time_last_dumped
