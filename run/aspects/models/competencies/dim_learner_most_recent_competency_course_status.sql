
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_learner_most_recent_competency_course_status`
        ("id", "actor_id", "group_id", "tag_id", "taxonomy_id", "course_key", "status_id", "status_name", "created", "modified", "time_last_dumped")-- One row per learner x competency x course run with the latest status of its
-- course-level criteria group, the only place AttemptedNotDemonstrated is stored.
-- openedx-core allows one course-level group per competency and course run.
-- ReplacingMergeTree(time_last_dumped) keeps the latest dump per key only after a
-- merge: read with FINAL.

select
    id,
    actor_id,
    group_id,
    tag_id,
    taxonomy_id,
    course_key,
    status_id,
    status_name,
    parseDateTime64BestEffort(created, 6) as created,
    parseDateTime64BestEffort(modified, 6) as modified,
    parseDateTime64BestEffort(time_last_dumped, 6) as time_last_dumped
from `event_sink`.`student_competency_course_status`
  
  
  