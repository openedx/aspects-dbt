{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("AggregatingMergeTree()"),
        order_by="(emission_hour)",
        partition_by="(toYYYYMM(emission_hour))",
    )
}}

select
    date_trunc('hour', emission_time) as emission_hour,
    uniqCombinedState(course_id) as courses_cnt
from {{ ref("xapi_events_all_parsed") }}
group by emission_hour
