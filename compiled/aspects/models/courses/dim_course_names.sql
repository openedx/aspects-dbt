

with
    latest as (
        select org, course_key, max(modified) as last_modified
        from `event_sink`.`course_overviews`
        group by org, course_key
    )
select
    course_overviews.course_key as course_key,
    course_overviews.display_name as course_name,
    splitByString('+', course_overviews.course_key)[-1] as course_run,
    course_overviews.org as org,
    JSONExtract(course_overviews.course_data_json, 'tags', 'String') as tags_str
from `event_sink`.`course_overviews` course_overviews
inner join
    latest
    on latest.org = course_overviews.org
    and latest.course_key = course_overviews.course_key
    and course_overviews.modified = latest.last_modified