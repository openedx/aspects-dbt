with
    final_results as (
        select
            org,
            course_key,
            actor_id,
            splitByChar('@', subsection_block_id)[3] as block_id_short,
            problem_id,
            success,
            scaled_score,
            {{ format_object_location("display_name_with_location") }},  -- object_location, object_name_location
            {{
                format_problem_location(
                    "object_id",
                    "display_name_with_location",
                )
            }},  -- problem_location, problem_name_location
            graded
        from {{ ref("dim_learner_last_response") }}
    )
select
    org,
    course_key,
    block_id_short,
    problem_id,  -- for primary key
    problem_location,
    problem_name_location,
    actor_id,
    success,
    object_location as subsection_number,
    object_name_location as subsection_with_name,
    scaled_score,
    graded
from final_results
