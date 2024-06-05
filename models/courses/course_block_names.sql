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
        lifetime=env_var("ASPECTS_BLOCK_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}
select
    location, block_name, course_key, graded, course_order, display_name_with_location
from {{ ref("most_recent_course_blocks") }}
