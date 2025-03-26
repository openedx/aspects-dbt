
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_learner_last_response`
        ("org", "course_key", "object_id", "problem_id", "actor_id", "interaction_type", "attempt", "success", "emission_time", "responses", "scaled_score")

select
    org,
    course_key,
    object_id,
    problem_id,
    actor_id,
    interaction_type,
    MAX(attempts) as attempt,
    argMax(success, attempts) as success,
    argMax(emission_time, attempts) as emission_time,
    argMax(responses, attempts) as responses,
    argMax(scaled_score, attempts) as scaled_score
from `xapi`.`problem_events`
where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
group by org, course_key, object_id, problem_id, actor_id, interaction_type
  
  
  