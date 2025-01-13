{{
    config(
        materialized="dictionary",
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
        lifetime=env_var("ASPECTS_BLOCK_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}
select
    location, block_name, course_key, graded, course_order, display_name_with_location
from {{ ref("dim_most_recent_course_blocks") }}
