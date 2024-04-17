{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("user_id", "Int32"),
            ("external_user_id", "UUID"),
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
        select
            user_id,
            name,
            email,
            ROW_NUMBER() over (
                partition by user_id order by (id, time_last_dumped) DESC
            ) as rn
        from {{ source("event_sink", "user_profile") }}
    )
select mrup.user_id as user_id, external_user_id, username, name, email
from {{ source("event_sink", "external_id") }} ex
left outer join most_recent_user_profile mrup on mrup.user_id = ex.user_id
where mrup.rn = 1
