

with
    events as (
        select
            event_id,
            cast(emission_time as DateTime) as emission_time,
            actor_id,
            object_id,
            course_key,
            org,
            verb_id,
            JSON_VALUE(event, '$.result.response') as responses,
            JSONExtractFloat(event, 'result', 'score', 'scaled') as scaled_score,
            if(
                verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated',
                cast(JSON_VALUE(event, '$.result.success') as Bool),
                false
            ) as success,
            toLowCardinality(
                JSON_VALUE(event, '$.object.definition.interactionType')
            ) as interaction_type,
            if(
                verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated',
                cast(
                    JSON_VALUE(
                        event,
                        '$.object.definition.extensions."http://id.tincanapi.com/extension/attempt-id"'
                    ) as Int16
                ),
                0
            ) as attempts,
            
    regexpExtract(
        object_id, 'xblock/([\w\d-\+:@]*@problem\+block@[\w\d][^_\/]*)(_\d_\d)?', 1
    )
 as problem_id
        from `xapi`.`xapi_events_all_parsed` xapi
        where
            verb_id in (
                'https://w3id.org/xapi/acrossx/verbs/evaluated',
                'http://adlnet.gov/expapi/verbs/passed',
                'http://adlnet.gov/expapi/verbs/asked'
            )
    )
select
    events.*,
    course_name,
    block_name,
    display_name_with_location,
    
    concat(
        '<a href="', object_id, '" target="_blank">', display_name_with_location, '</a>'
    )
 as problem_link,
    graded,
    course_order
from events
left join
    `xapi`.`dim_course_blocks` blocks
    on (events.course_key = blocks.course_key and events.problem_id = blocks.block_id)