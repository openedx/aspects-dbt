{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("course_name", "String"),
            ("course_run", "String"),
            ("org", "String"),
            ("tags_str", "String"),
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

with
    latest as (
        select org, course_key, max(modified) as last_modified
        from {{ source("event_sink", "course_overviews") }}
        group by org, course_key
    )
select
    course_key,
    display_name as course_name,
    splitByString('+', course_key)[-1] as course_run,
    org,
    JSONExtract(course_data_json, 'tags', 'String') as tags_str
from {{ source("event_sink", "course_overviews") }} co
inner join
    latest mr
    on mr.org = co.org
    and mr.course_key = co.course_key
    and co.modified = mr.last_modified
