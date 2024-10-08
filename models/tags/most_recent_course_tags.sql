with
    most_recent_overviews as (
        select org, course_key, max(time_last_dumped) as last_modified
        from {{ source("event_sink", "course_overviews") }}
        group by org, course_key
    ),
    most_recent_course_tags as (
        select
            course_key,
            display_name as course_name,
            splitByString('+', course_key)[-1] as course_run,
            org,
            JSONExtract(course_data_json, 'tags', 'String') as tags_str
        from {{ source("event_sink", "course_overviews") }} co
        inner join
            most_recent_overviews mro
            on co.org = mro.org
            and co.course_key = mro.course_key
            and co.time_last_dumped = mro.last_modified
    ),
    parsed_tags as (
        select
            course_key,
            course_name,
            arrayJoin(JSONExtractArrayRaw(tags_str))::Int32 as tag_id
        from most_recent_course_tags
    )
select course_key, course_name, tag_id, value as tag, lineage, mrt.name as taxonomy_name
from parsed_tags
inner join {{ ref("most_recent_tags") }} mrot FINAL on mrot.id = tag_id
inner join {{ ref("most_recent_taxonomies") }} mrt FINAL on mrt.id = mrot.taxonomy
