-- Fails if the last fact_video_watch_intervals refresh errored on any replica.
{% set intervals = ref("fact_video_watch_intervals") %}
{% set cluster = env_var("CLICKHOUSE_CLUSTER_NAME", "") %}

select hostName() as host, view, status, exception
from
    {% if cluster %} clusterAllReplicas('{{ cluster }}', system.view_refreshes)
    {% else %} system.view_refreshes
    {% endif %}
where
    database = '{{ intervals.schema }}'
    and view = '{{ intervals.identifier }}_mv'
    and exception != ''
