-- These macros create our per-view|table row based permissions.
-- Generally we will want to apply both of these to a model!

-- Allow all rows to admins and superusers
{% macro apply_admin_rbac(target_model) %}
CREATE ROW POLICY OR REPLACE openedx_policy_administrator ON
{{ target_model }}
USING 1 TO openedx_administrator, openedx_superuser;
{% endmacro %}

-- Only allow instructors to see courses they are staff on
{% macro apply_instructor_rbac(target_model) %}
CREATE ROW POLICY OR REPLACE openedx_policy_instructor ON
{{ target_model }}
USING
    course_id in (
        SELECT course_id
    FROM {{ source("superset_permissions", "superset_user_course_mapping") }}
    WHERE username=currentUser())
TO openedx_instructor;
{% endmacro %}
