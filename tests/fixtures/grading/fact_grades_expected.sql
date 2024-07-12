select 
    emission_time,
    org,
    course_key,
    course_name,
    course_run,
    entity_id,
    entity_name,
    if(entity_name_with_location != '', entity_name_with_location, null) as entity_name_with_location,
    grade_type,
    actor_id,
    scaled_score,
    grade_bucket,
    username,
    name,
    email
from fact_grades_expected
