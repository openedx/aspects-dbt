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

    {{
        print(
            "Running remove_deprecated_models on "
            ~ xapi
            ~ ", "
            ~ reporting
            ~ ", "
            ~ event_sink
            ~ "."
        )
    }}

    -- https://github.com/openedx/aspects-dbt/pull/111/
    {% set models_to_drop = [
        ("view", reporting, "fact_completions"),
        ("view", xapi, "fact_forum_interactions"),
        ("view", reporting, "fact_grades"),
        ("view", reporting, "learner_summary"),
        ("view", reporting, "fact_navigation_dropoff"),
        ("view", reporting, "fact_learner_problem_summary"),
        ("view", reporting, "fact_problem_engagement"),
        ("view", reporting, "fact_problem_engagement_per_subsection"),
        ("view", reporting, "fact_problem_responses_extended"),
        ("view", reporting, "dim_at_risk_learners"),
        ("view", reporting, "fact_transcript_usage"),
        ("view", reporting, "fact_watched_video_segments"),
        ("view", reporting, "dim_course_blocks_extended"),
        ("view", reporting, "fact_navigation"),
        ("view", reporting, "int_pages_per_subsection"),
        ("view", reporting, "int_problems_per_subsection"),
        ("view", reporting, "fact_problem_responses"),
        ("view", reporting, "int_problem_hints"),
        ("view", reporting, "int_problem_results"),
        ("view", reporting, "most_recent_course_tags"),
        ("view", reporting, "fact_student_status"),
        ("view", reporting, "watched_video_duration"),
        ("mv", event_sink, "course_enrollment"),
        ("mv", event_sink, "course_relationships"),
        ("mv", xapi, "forum_events"),
        ("mv", reporting, "video_transcript_events"),
        ("mv", reporting, "fact_learner_course_grade"),
        ("mv", reporting, "fact_learner_course_status"),
        ("mv", event_sink, "most_recent_course_blocks"),
        ("mv", reporting, "fact_enrollment_status"),
        ("mv", event_sink, "most_recent_object_tags"),
        ("mv", event_sink, "most_recent_tags"),
        ("mv", event_sink, "most_recent_taxonomies"),
        ("mv", xapi, "fact_learner_last_course_visit"),
        ("mv", xapi, "responses"),
        ("mv", xapi, "section_page_engagement"),
        ("mv", xapi, "section_problem_engagement"),
        ("mv", xapi, "section_video_engagement"),
        ("mv", xapi, "subsection_page_engagement"),
        ("mv", xapi, "subsection_problem_engagement"),
        ("mv", xapi, "subsection_video_engagement"),
        ("mv", xapi, "fact_instance_enrollments"),
        ("mv", reporting, "int_videos_per_subsection"),
        ("dictionary", event_sink, "course_names"),
        ("dictionary", event_sink, "course_block_names"),
        ("dictionary", event_sink, "course_tags"),
    ] %}

    {% for model in models_to_drop %}
        {% do do_drop(model[0], model[1], model[2]) %}
    {% endfor %}

{% endmacro %}
