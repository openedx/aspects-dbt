select 
    cast(external_user_id as UUID) as external_user_id, 
    username, 
    name, 
    email 
from dim_user_pii_seed
