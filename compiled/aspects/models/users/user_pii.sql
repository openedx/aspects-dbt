

with
    most_recent_user_profile as (
        select
            user_id,
            name,
            email,
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
        from `event_sink`.`user_profile`
    )
select
    mrup.user_id as user_id,
    external_user_id,
    external_id_type,
    username,
    name,
    email,
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
from `event_sink`.`external_id` ex
left outer join
    most_recent_user_profile mrup
    on mrup.user_id = ex.user_id
    and (ex.external_id_type = 'xapi' or ex.external_id_type is NULL)
where mrup.rn = 1