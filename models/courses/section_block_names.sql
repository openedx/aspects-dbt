{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("section_id", "Int32"),
            ("location", "String"),
            ("hierarchy_location", "String"),
            ("section_name_with_location", "String"),
        ],
        primary_key="course_key, section_id",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime="120",
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

select
    course_key as course_key,
    section_id as section_id,
    location as location,
    toString(section_id) || ':0:0' as hierarchy_location,
    hierarchy_location || ' - ' || block_name as section_name_with_location
from {{ ref("course_block_names") }}
where location like '%@chapter+block@%'
