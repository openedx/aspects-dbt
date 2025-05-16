

with
    final_results as (
        select
            first_success.org as org,
            first_success.course_key as course_key,
            first_success.success as success,
            first_success.attempt as attempt,
            first_success.actor_id as actor_id,
            splitByChar('@', block_id)[3] as block_id_short,
            
    substring(
        regexpExtract(object_id, '(@problem\+block@[\w\d][^_\/]*)(_\d)?', 2), 2
    ) as _problem_id_number,
    ifNull(nullIf(_problem_id_number, ''), '1') as _problem_id_or_1,
    splitByString(' - ', display_name_with_location) as _problem_with_name,
    arrayStringConcat(
        arrayMap(
            x -> (leftPad(x, 2, char(917768))),
            splitByString(':', _problem_with_name[1])
        ),
        ':'
    ) as _problem_number,
    concat(_problem_number, '_', _problem_id_or_1) as problem_number,
    concat(problem_number, ' - ', _problem_with_name[2]) as problem_name_location

        from `xapi`.`dim_learner_first_success_response` as first_success
        join
            `xapi`.`dim_course_blocks` as blocks
            on (
                first_success.course_key = blocks.course_key
                and first_success.problem_id = blocks.block_id
            )
    )
select
    org,
    course_key,
    success,
    attempt,
    actor_id,
    problem_number,
    problem_name_location,
    block_id_short
from final_results