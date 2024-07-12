select 
emission_date,
org,
course_key,
rollup_name,
block_name,
course_order,
actor_id,
cast(total_views as UInt64) as total_views,
username,
name,
email
from fact_navigation_dropoff_expected
