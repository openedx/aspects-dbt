{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("location", "String"),
            ("course_key", "String"),
            ("block_name", "String"),
            ("display_name_with_location", "String"),
            ("section_id", "Int32"),
            ("subsection_id", "Int32"),
            ("unit_id", "Int32"),
            ("course_order", "Int32"),
            ("graded", "Bool"),
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

with
    most_recent_course_blocks as (
        select
            location as location,
            course_key as course_key,
            display_name as block_name,
            toString(section_id)
            || ':'
            || toString(subsection_id)
            || ':'
            || toString(unit_id) as hierarchy_location,
            hierarchy_location || ' - ' || block_name as display_name_with_location,
            JSONExtractInt(xblock_data_json, 'section') as section_id,
            JSONExtractInt(xblock_data_json, 'subsection') as subsection_id,
            JSONExtractInt(xblock_data_json, 'unit') as unit_id,
            `order` as course_order,
            JSONExtractBool(xblock_data_json, 'graded') as graded,
            time_last_dumped,
            row_number() over (
                partition by location order by time_last_dumped desc
            ) as rn
        from {{ source("event_sink", "course_blocks") }}
    )
select
    location as location,
    course_key as course_key,
    block_name as block_name,
    display_name_with_location as display_name_with_location,
    section_id as section_id,
    subsection_id as subsection_id,
    unit_id as unit_id,
    course_order as course_order,
    graded as graded
from most_recent_course_blocks
where rn = 1
