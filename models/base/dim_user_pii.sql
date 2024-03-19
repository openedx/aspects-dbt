select
    external_user_id,
    external_id_type,
    username,
    name,
    language,
    year_of_birth,
    gender,
    level_of_education,
    country,
    if(
        toInt32OrZero(year_of_birth) = 0,
        NULL,
        toYear(now()) - toInt32OrZero(year_of_birth)
    ) as age
from {{ source("event_sink", "user_pii") }} user_pii
