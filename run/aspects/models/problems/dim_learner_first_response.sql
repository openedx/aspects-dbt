
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_learner_first_response`
        ("org", "course_key", "object_id", "problem_id", "actor_id", "interaction_type", "attempt", "success", "emission_time", "responses", "scaled_score")

select
    org,
    course_key,
    object_id,
    problem_id,
    actor_id,
    interaction_type,
    MIN(attempts) as attempt,
    argMin(success, attempts) as success,
    argMin(emission_time, attempts) as emission_time,
    argMin(responses, attempts) as responses,
    argMin(scaled_score, attempts) as scaled_score
from `xapi`.`problem_events`
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
group by org, course_key, object_id, problem_id, actor_id, interaction_type
  
  
  