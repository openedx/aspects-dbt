select external_user_id, username, name, email from {{ ref("user_pii") }} user_pii
