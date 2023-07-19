/*
 Returns the most recent course overview row for each course.
 Because the modified timestamp is high resolution we shouldn't have duplicate
 rows for the same org/course_key, but sometimes they show up in testing due to
 a bug in the xapi-db-load script.
*/
with most_recent_overviews as (
    select org, course_key, max(modified) as last_modified
    from {{ source('event_sink', 'course_overviews') }}
    group by org, course_key
)
select
    org,
    course_key,
    display_name,
    course_start,
    course_end,
    enrollment_start,
    enrollment_end,
    self_paced,
    created,
    modified as last_modified
from {{ source('event_sink', 'course_overviews') }} co
inner join most_recent_overviews mro on
    co.org = mro.org AND
    co.course_key = mro.course_key AND
    co.modified = mro.last_modified
