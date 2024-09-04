{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("tag", "String"),
            ("course_name", "String"),
            ("taxonomy_name", "String"),
            ("lineage", "String"),
        ],
        primary_key="(course_key, tag)",
        layout="COMPLEX_KEY_HASHED()",
        lifetime=env_var("ASPECTS_COURSE_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

select course_key, tag, course_name, taxonomy_name, lineage
from {{ ref("most_recent_course_tags") }}
order by course_key
