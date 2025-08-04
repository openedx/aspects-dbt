{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(course_key, block_id_short)",
        order_by="(course_key, block_id_short, response)",
    )
}}

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
        from {{ ref("problem_events") }}
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
            {{
                format_problem_number_location(
                    "first_response.object_id", "blocks.display_name_with_location"
                )
            }}
        from first_response
        left join
            {{ ref("dim_course_blocks") }} blocks
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
