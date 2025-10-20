
  
    
  
    
    
    
        
         


        insert into `xapi`.`dim_subsection_performance`
        ("org", "course_key", "block_id", "total_avg", "score_range", "score_range_count")

with
    avg_actor as (
        select org, course_key, block_id_short, avg(scaled_score) as avg_score, actor_id
        from `xapi`.`dim_subsection_problem_results`
        group by org, course_key, block_id_short, actor_id
    ),
    avg_total as (
        select org, course_key, block_id_short, avg(scaled_score) as total_avg
        from `xapi`.`dim_subsection_problem_results`
        group by org, course_key, block_id_short
    ),
    score_ranges as (
        select
            org,
            course_key,
            block_id_short,
            case
                when round(avg_score * 100, 2) > 90
                then '>90%'
                when round(avg_score * 100, 2) > 70
                then '71-90%'
                when round(avg_score * 100, 2) > 50
                then '51-70%'
                when round(avg_score * 100, 2) > 30
                then '31-50%'
                when round(avg_score * 100, 2) > 0
                then '1-30%'
                else '0%'
            end as score_range,
            count(1) as score_range_count
        from avg_actor
        group by org, course_key, block_id_short, score_range
    )
select
    score_ranges.org as org,
    score_ranges.course_key as course_key,
    score_ranges.block_id_short as block_id,
    cast(round(avg_total.total_avg, 2) as Float32) as total_avg,
    score_ranges.score_range as score_range,
    score_ranges.score_range_count as score_range_count
from score_ranges
join avg_total using (org, course_key, block_id_short)
  
  
  