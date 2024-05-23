

  create view `xapi`.`fact_problem_engagement` 
  
    
    
  as (
    with
    subsection_engagement as (
        select
            org,
            course_key,
            'subsection' as content_level,
            actor_id,
            subsection_block_id as block_id,
            engagement_level as section_subsection_problem_engagement
        from `xapi`.`subsection_problem_engagement`
    ),
    section_engagement as (
        select
            org,
            course_key,
            'section' as content_level,
            actor_id,
            section_block_id as block_id,
            engagement_level as section_subsection_problem_engagement
        from `xapi`.`section_problem_engagement`
    ),
    problem_engagement as (
        select *
        from subsection_engagement
        union all
        select *
        from section_engagement
    )
select
    pe.org as org,
    pe.course_key as course_key,
    course_blocks.course_run as course_run,
    course_blocks.display_name_with_location as section_subsection_name,
    pe.content_level as content_level,
    pe.actor_id as actor_id,
    pe.section_subsection_problem_engagement as section_subsection_problem_engagement,
    users.username as username,
    users.name as name,
    users.email as email
from problem_engagement pe
join
    `xapi`.`dim_course_blocks` course_blocks
    on (
        pe.org = course_blocks.org
        and pe.course_key = course_blocks.course_key
        and pe.block_id = course_blocks.block_id
    )
left outer join
    `xapi`.`dim_user_pii` users on toUUID(pe.actor_id) = users.external_user_id
  )
      
      
                    -- end_of_sql
                    
                    