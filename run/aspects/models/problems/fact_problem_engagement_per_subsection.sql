

  create view `xapi`.`fact_problem_engagement_per_subsection` 
  
    
    
  as (
    with
    attempted_subsection_problems as (
        select distinct
            date(emission_time) as attempted_on,
            org,
            course_key,
            course_run,
            
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

            as subsection_number,
            course_order as course_order,
            graded,
            actor_id,
            problem_id,
            username,
            name,
            email
        from `xapi`.`fact_problem_responses`
    )

select
    attempts.org as org,
    attempts.course_key as course_key,
    attempts.course_run as course_run,
    problems.section_with_name as section_with_name,
    problems.subsection_with_name as subsection_with_name,
    problems.item_count as item_count,
    attempts.actor_id as actor_id,
    attempts.problem_id as problem_id,
    attempts.username as username,
    attempts.name as name,
    attempts.email as email
from attempted_subsection_problems attempts
join
    `xapi`.`int_problems_per_subsection` problems
    on (
        attempts.org = problems.org
        and attempts.course_key = problems.course_key
        and attempts.section_number = problems.section_number
        and attempts.subsection_number = problems.subsection_number
    )
  )
      
      
                    -- end_of_sql
                    
                    