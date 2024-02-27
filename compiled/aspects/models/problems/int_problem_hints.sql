with
    hints as (
        select
            emission_time,
            org,
            course_key,
            
    regexpExtract(
        object_id, 'xblock/([\w\d-\+:@]*@problem\+block@[\w\d][^_]*)(_\d_\d)?', 1
    )
 as problem_id,
            actor_id,
            case
                when object_id like '%/hint%'
                then 'hint'
                when object_id like '%/answer%'
                then 'answer'
                else 'N/A'
            end as help_type
        from `xapi`.`problem_events`
        where verb_id = 'http://adlnet.gov/expapi/verbs/asked'
    )

select
    hints.emission_time as emission_time,
    hints.org as org,
    hints.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    hints.problem_id as problem_id,
    blocks.block_name as problem_name,
    blocks.display_name_with_location as problem_name_with_location,
    hints.actor_id as actor_id,
    hints.help_type as help_type
from hints
join
    `xapi`.`dim_course_blocks` blocks
    on (hints.course_key = blocks.course_key and hints.problem_id = blocks.block_id)