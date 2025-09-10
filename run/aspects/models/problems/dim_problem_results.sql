
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_problem_results`
        ("org", "course_key", "success", "attempts", "actor_id", "problem_number", "problem_name_location", "block_id_short")

with
    first_success as (
        select
            org,
            course_key,
            object_id,
            argMin(attempts, emission_time) as attempts,
            success,
            actor_id
        from `xapi`.`problem_events`
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated' and success
        group by org, course_key, object_id, actor_id, success
    ),
    events as (
        select distinct org, course_key, object_id, problem_id
        from `xapi`.`problem_events` events
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
    ),
    final_results as (
        select
            events.org as org,
            events.course_key as course_key,
            first_success.success as success,
            first_success.attempts as attempts,
            first_success.actor_id as actor_id,
            splitByChar('@', events.problem_id)[3] as block_id_short,
            
    substring(
        regexpExtract(events.object_id, '(@problem\+block@[\w\d][^_\/]*)(_\d)?', 2),
        2
    ) as _problem_id_number,
    ifNull(nullIf(_problem_id_number, ''), '1') as _problem_id_or_1,
    splitByString(' - ', blocks.display_name_with_location) as _problem_with_name,
    arrayStringConcat(
        arrayMap(
            x -> (leftPad(x, 2, char(917768))),
            splitByString(':', _problem_with_name[1])
        ),
        ':'
    ) as _problem_number,
    concat(_problem_number, '_', _problem_id_or_1) as problem_number,
    concat(problem_number, ' - ', _problem_with_name[2]) as problem_name_location

        from events
        left join
            `xapi`.`dim_course_blocks` blocks
            on (
                events.course_key = blocks.course_key
                and events.problem_id = blocks.block_id
            )
        left join
            first_success
            on (
                first_success.org = events.org
                and first_success.course_key = events.course_key
                and first_success.object_id = events.object_id
            )
    )
select distinct
    org,
    course_key,
    success,
    attempts,
    actor_id,
    problem_number,
    problem_name_location,
    block_id_short
from final_results
  
  
  