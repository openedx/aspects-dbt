{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("course_name", "String"),
            ("course_run", "String"),
            ("tags_str", "String"),
            ("org", "String"),
        ],
        primary_key="course_key",
        layout="COMPLEX_KEY_HASHED()",
        lifetime=env_var("ASPECTS_COURSE_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

select
    course_key,
    argMax(display_name, modified) as course_name,
    splitByString('+', course_key)[-1] as course_run,
    JSONExtract(argMax(course_data_json, modified), 'tags', 'String') as tags_str,
    org
from {{ source("event_sink", "course_overviews") }}
group by org, course_key, course_data_json