select
    emission_day,
    cast(uniqCombinedMerge(events_cnt) as Int32) as events_cnt
from {{ ref("fact_instance_events") }}
group by emission_day