{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("location", "String"),
            ("block_name", "String"),
            ("course_key", "String"),
            ("graded", "Bool"),
            ("display_name_with_location", "String"),
        ],
        primary_key="location",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime="120",
        source_type="clickhouse",
    )
}}

select
    location, display_name as block_name, course_key, graded, display_name_with_location
from {{ ref("most_recent_course_blocks") }}
