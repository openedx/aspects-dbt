{% macro do_drop(type, schema, relation) %}
    -- Drop a relation, types are "view", "table", or "mv".
    -- "mv" will drop both the expected view and destination table.
    {% if type == "mv" %}
        {% do do_drop("view", schema, relation ~ "_mv") %}
        {% do do_drop("table", schema, relation) %}
    {% else %}
        {% set cmd = "drop " ~ type ~ " if exists " ~ schema ~ "." ~ relation ~ ";" %}
        {% print(cmd) %}
        {% do run_query(cmd) %}
    {% endif %}
{% endmacro %}

{% macro remove_deprecated_models() %}
    {% set xapi = env_var("ASPECTS_XAPI_DATABASE", "xapi") %}
    {% set reporting = env_var("DBT_PROFILE_TARGET_DATABASE", "reporting") %}
    {% set event_sink = env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink") %}

    {{ print("Running remove_deprecated_models on " ~ xapi ~ ", " ~ reporting ~ ", " ~ event_sink ~ ".") }}

    -- https://github.com/openedx/aspects-dbt/pull/111/
    {% do do_drop("mv", xapi, "completion_events") %}
    {% do do_drop("view", reporting, "fact_completions") %}
    {% do do_drop("view", xapi, "fact_forum_interactions") %}
    {% do do_drop("mv", xapi, "forum_events") %}
    {% do do_drop("view", reporting, "fact_grades") %}
    {% do do_drop("view", reporting, "learner_summary") %}
    {% do do_drop("view", reporting, "fact_navigation_dropoff") %}
    {% do do_drop("view", reporting, "fact_learner_problem_summary") %}
    {% do do_drop("view", reporting, "fact_problem_engagement") %}
    {% do do_drop("view", reporting, "fact_problem_engagement_per_subsection") %}
    {% do do_drop("view", reporting, "fact_problem_responses_extended") %}
    {% do do_drop("view", reporting, "dim_at_risk_learners") %}
    {% do do_drop("view", reporting, "fact_transcript_usage") %}
    {% do do_drop("view", reporting, "fact_watched_video_segments") %}
    {% do do_drop("mv", reporting, "video_transcript_events") %}

{% endmacro %}
