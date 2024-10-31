{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("user_id", "Int32"),
            ("external_user_id", "String"),
            ("username", "String"),
            ("name", "String"),
            ("email", "String"),
        ],
        primary_key="(user_id, external_user_id)",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime=env_var("ASPECTS_PII_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

with
    most_recent_user_profile as (
        select user_id, max(time_last_dumped) as time_last_dumped
        from {{ source("event_sink", "user_profile") }}
        group by user_id
    )
select
    ex.user_id as user_id,
    if(
        empty(ex.external_user_id),
        concat('mailto:', email),
        ex.external_user_id::String
    ) as external_user_id,
    up.username,
    up.name,
    up.email
from most_recent_user_profile mrup
left outer join
    {{ source("event_sink", "external_id") }} ex on mrup.user_id = ex.user_id
left outer join
    {{ source("event_sink", "user_profile") }} up
    on up.user_id = mrup.user_id
    and up.time_last_dumped = mrup.time_last_dumped
