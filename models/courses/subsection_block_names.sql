{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("section_id", "Int32"),
            ("subsection_id", "Int32"),
            ("location", "String"),
            ("subsection_name_with_location", "String"),
        ],
        primary_key="course_key, section_id, subsection_id",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime=env_var("ASPECTS_BLOCK_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

select
    course_key as course_key,
    section_id as section_id,
    subsection_id as subsection_id,
    location as location,
    display_name_with_location as subsection_name_with_location
from {{ ref("course_block_names") }}
where location like '%@sequential+block@%'
