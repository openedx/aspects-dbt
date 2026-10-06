-- Fails while the last refresh of fact_video_watch_intervals errored on any
-- replica. Refresh errors otherwise only reach system.view_refreshes, and
-- SYSTEM WAIT VIEW has no ON CLUSTER.
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
