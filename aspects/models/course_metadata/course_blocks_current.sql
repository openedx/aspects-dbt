/*
Returns the most recent course block metadata row for each course xblock.
*/
with most_recent_blocks as (
    select org, course_key, location, max(edited_on) as last_modified
    from {{ source('event_sink', 'course_blocks') }}
    group by org, course_key, location
)
select count(*) from (
select
    org,
    course_key,
    location,
    display_name,
    order,
    edited_on as last_modified
from {{ source('event_sink', 'course_blocks') }} cb
inner join most_recent_blocks mrb on
    cb.org = mrb.org AND
    cb.course_key = mrb.course_key AND
    cb.location = mrb.location AND
    cb.edited_on = mrb.last_modified)
