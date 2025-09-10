
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_problem_responses`
        ("org", "course_key", "emission_time", "block_id_short", "response", "success", "interaction_type", "problem_number", "problem_name_location", "response_count")

with
    first_response as (
        select
            org,
            course_key,
            object_id,
            problem_id,
            actor_id,
            interaction_type,
            argMin(success, attempts) as success,
            argMin(emission_time, attempts) as emission_time,
            argMin(responses, attempts) as responses,
            argMin(scaled_score, attempts) as scaled_score
        from `xapi`.`problem_events`
        where verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
        group by org, course_key, object_id, problem_id, actor_id, interaction_type
    ),
    final_results as (
        select
            first_response.org as org,
            first_response.course_key as course_key,
            first_response.emission_time as emission_time,
            splitByChar('@', first_response.problem_id)[3] as block_id_short,
            replaceRegexpAll(
                first_response.responses,
                '<.*?hint.*?<\/.*?hint>|</div>|<div>|\[|\]',
                ''
            ) as _response1,
            replaceRegexpAll(_response1, '",(\s|)"', ',') as _response2,
            case
                when first_response.responses like '[%'
                then arrayJoin(splitByChar(',', replaceAll(_response2, '"', '')))
                else _response2
            end as response,
            first_response.success as success,
            first_response.interaction_type as interaction_type,
            
    substring(
        regexpExtract(first_response.object_id, '(@problem\+block@[\w\d][^_\/]*)(_\d)?', 2),
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

        from first_response
        left join
            `xapi`.`dim_course_blocks` blocks
            on (
                first_response.course_key = blocks.course_key
                and first_response.problem_id = blocks.block_id
            )
    )
select
    org,
    course_key,
    emission_time,
    block_id_short,
    response,
    success,
    interaction_type,
    problem_number,
    problem_name_location,
    count(1) as response_count
from final_results
group by
    org,
    course_key,
    emission_time,
    block_id_short,
    response,
    success,
    interaction_type,
    problem_number,
    problem_name_location
  
  
  