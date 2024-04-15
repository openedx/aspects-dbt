{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("course_key", "String"),
            ("course_name", "String"),
            ("course_run", "String"),
            ("org", "String"),
        ],
        primary_key="course_key",
        layout="COMPLEX_KEY_HASHED()",
        lifetime="120",
        source_type="clickhouse",
    )
}}

with
    most_recent_overviews as (
        select org, course_key, max(modified) as last_modified
        from {{ source("event_sink", "course_overviews") }}
        group by org, course_key
    )
select course_key, display_name, splitByString('+', course_key)[-1] as course_run, org
from {{ source("event_sink", "course_overviews") }} co
inner join
    most_recent_overviews mro
    on co.org = mro.org
    and co.course_key = mro.course_key
    and co.modified = mro.last_modified
