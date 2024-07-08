select
    emission_hour,
    cast(uniqCombinedMerge(courses_cnt) as Int32) as courses_cnt
from {{ ref("fact_instance_courses") }}
group by emission_hour