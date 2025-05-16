

with
    parsed_tags as (
        select
            course_key,
            course_name,
            arrayJoin(JSONExtractArrayRaw(tags_str))::Int32 as tag_id
        from `xapi`.`dim_course_names`
    )
select course_key, value as tag, course_name, mrt.name as taxonomy_name, lineage, tag_id
from parsed_tags
inner join `xapi`.`dim_most_recent_tags` mrot FINAL on mrot.id = tag_id
inner join `xapi`.`dim_most_recent_taxonomies` mrt FINAL on mrt.id = mrot.taxonomy