-- select one record per (learner, problem, help type)
-- contains the number of times the learner has asked for hints or
-- answers for the given problem
{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(problem_id, actor_id)",
        partition_by="toYYYYMM(emission_time)",
        ttl=env_var("ASPECTS_DATA_TTL_EXPRESSION", ""),
    )
}}

with
    hints as (
        select
            MAX(emission_time) as emission_time,
            problem_id,
            actor_id,
            case
                when object_id like '%/hint%'
                then 'hint'
                when object_id like '%/answer%'
                then 'answer'
                else 'N/A'
            end as help_type,
            count(*) as num_help_interactions
        from {{ ref("problem_events") }}
        where verb_id = 'http://adlnet.gov/expapi/verbs/asked'
        group by problem_id, actor_id, help_type
    )

select *
from hints
