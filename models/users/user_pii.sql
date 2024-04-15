{{
    config(
        materialized="dictionary",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        fields=[
            ("user_id", "Int32"),
            ("external_user_id", "UUID"),
            ("external_id_type", "String"),
            ("username", "String"),
            ("name", "String"),
            ("meta", "String"),
            ("courseware", "String"),
            ("language", "String"),
            ("location", "String"),
            ("year_of_birth", "String"),
            ("gender", "String"),
            ("level_of_education", "String"),
            ("mailing_address", "String"),
            ("city", "String"),
            ("country", "String"),
            ("state", "String"),
            ("goals", "String"),
            ("bio", "String"),
            ("profile_image_uploaded_at", "String"),
            ("phone_number", "String"),
        ],
        primary_key="(user_id, external_user_id)",
        layout="COMPLEX_KEY_SPARSE_HASHED()",
        lifetime=env_var("ASPECTS_PII_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
    )
}}

with
    most_recent_user_profile as (
        select
            user_id,
            name,
            meta,
            courseware,
            language,
            location,
            year_of_birth,
            gender,
            level_of_education,
            mailing_address,
            city,
            country,
            state,
            goals,
            bio,
            profile_image_uploaded_at,
            phone_number,
            ROW_NUMBER() over (
                partition by user_id order by (id, time_last_dumped) DESC
            ) as rn
        from {{ source("event_sink", "user_profile") }}
    )
select
    mrup.user_id as user_id,
    external_user_id,
    external_id_type,
    username,
    name,
    meta,
    courseware,
    language,
    location,
    year_of_birth,
    gender,
    level_of_education,
    mailing_address,
    city,
    country,
    state,
    goals,
    bio,
    profile_image_uploaded_at,
    phone_number
from {{ source("event_sink", "external_id") }} ex
right outer join
    most_recent_user_profile mrup
    on mrup.user_id = ex.user_id
    and (ex.external_id_type = 'xapi' or ex.external_id_type is NULL)
where mrup.rn = 1
