select emission_time,org,course_key,course_name,course_run,entity_id,entity_name,
case when entity_name_with_location = '\N' then null else entity_name_with_location end as entity_name_with_location,
grade_type,actor_id,scaled_score,grade_bucket,username,name,email
from reporting.fact_grades_expected