-- Return an ON CLUSTER clause if we are running dbt against a cluster
{% macro on_cluster() -%}
    {%- if env_var("CLICKHOUSE_CLUSTER_NAME", "") != "" -%}
    ON CLUSTER {{ env_var("CLICKHOUSE_CLUSTER_NAME") }}
  {%- endif -%}
{%- endmacro %}

-- Return a version of the given table engine suitable for use on ClickHouse clusters
{% macro get_engine(engine) -%}
  {%- if env_var("CLICKHOUSE_CLUSTER_NAME", "") != "" -%}
    {{ "Replicated" + engine }}
  {% else %}
    {{ engine }}
  {%- endif -%}
{%- endmacro %}
