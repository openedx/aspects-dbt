{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(year, month_name)",
        order_by="(year, month_name)",
        partition_by="",
    )
}}


with
    grades_average as (
        select
            course_key,
            monthName(emission_time) as month_name,
            toYear(emission_time) as year,
            case
                when verb_id = 'http://adlnet.gov/expapi/verbs/passed' then 1 else 0
            end as approving,
            count(1) as total
        from xapi.grading_events
        where
            verb_id in (
                'http://adlnet.gov/expapi/verbs/passed',
                'http://adlnet.gov/expapi/verbs/failed'
            )
        group by course_key, year, month_name, approving
        having approving = 1
    )

select year, month_name, avg(total)
from grades_average
group by year, month_name
