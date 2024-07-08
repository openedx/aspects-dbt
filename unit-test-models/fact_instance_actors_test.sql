select
    emission_day,
    cast(uniqCombinedMerge(actors_cnt) as Int32) as actors_cnt
from {{ ref("fact_instance_actors") }}
group by emission_day
