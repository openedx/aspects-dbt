

  create view `xapi`.`fact_problem_responses_extended` 
  
    
    
  as (
    with
    problem_results as (
        select
            *,
            
    concat(
        splitByString(
            ':', splitByString(' - ', problem_name_with_location)[1], 1
        )[1],
        ':0:0'
    )
 as section_number,
            
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', problem_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

            as subsection_number
        from `xapi`.`int_problem_results`
    )
select
    results.emission_time as emission_time,
    results.org as org,
    results.course_key as course_key,
    results.course_name as course_name,
    results.course_run as course_run,
    problems.section_with_name as section_with_name,
    problems.subsection_with_name as subsection_with_name,
    results.problem_id as problem_id,
    results.problem_name as problem_name,
    results.problem_name_with_location as problem_name_with_location,
    results.problem_link as problem_link,
    results.actor_id as actor_id,
    results.responses as responses,
    results.success as success,
    results.attempts as attempts,
    results.course_order as course_order,
    results.graded as graded,
    results.interaction_type as interaction_type,
    users.username as username,
    users.name as name,
    users.email as email
from problem_results results
join
    `xapi`.`int_problems_per_subsection` problems
    on (
        results.org = problems.org
        and results.course_key = problems.course_key
        and results.section_number = problems.section_number
        and results.subsection_number = problems.subsection_number
    )
left outer join
    `xapi`.`dim_user_pii` users on toUUID(actor_id) = users.external_user_id
  )
      
      
                    -- end_of_sql
                    
                    