

with
    final_results as (
        select
            last_response.org as org,
            last_response.course_key as course_key,
            last_response.success as success,
            last_response.attempts as attempts,
            last_response.actor_id as actor_id,
            splitByChar('@', last_response.problem_id)[3] as block_id_short,
            
    substring(
        regexpExtract(last_response.object_id, '(@problem\+block@[\w\d][^_\/]*)(_\d)?', 2),
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

        from `xapi`.`dim_learner_last_response` last_response
        left join
            `xapi`.`dim_course_blocks` blocks
            on (
                last_response.course_key = blocks.course_key
                and last_response.problem_id = blocks.block_id
            )
    )
select
    org,
    course_key,
    success,
    attempts,
    actor_id,
    problem_number,
    problem_name_location,
    block_id_short
from final_results