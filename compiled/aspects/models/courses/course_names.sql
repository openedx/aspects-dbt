

with
    most_recent_overviews as (
        select org, course_key, max(modified) as last_modified
        from `event_sink`.`course_overviews`
        group by org, course_key
    )
select
    course_key,
    display_name as course_name,
    splitByString('+', course_key)[-1] as course_run,
    org
from `event_sink`.`course_overviews` co
inner join
    most_recent_overviews mro
    on co.org = mro.org
    and co.course_key = mro.course_key
    and co.modified = mro.last_modified