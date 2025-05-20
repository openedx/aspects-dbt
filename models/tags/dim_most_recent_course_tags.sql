{{
    config(
        materialized="dictionary",
        fields=[
            ("course_key", "String"),
            ("tag", "String"),
            ("course_name", "String"),
            ("taxonomy_name", "String"),
            ("lineage", "String"),
            ("tag_id", "Int32"),
        ],
        primary_key="(course_key, tag)",
        layout="COMPLEX_KEY_HASHED()",
        lifetime=env_var("ASPECTS_COURSE_NAME_CACHE_LIFETIME", "120"),
        source_type="clickhouse",
        connection_overrides={
            "host": "localhost",
        },
    )
}}

with
    parsed_tags as (
        select
            course_key,
            course_name,
            arrayJoin(JSONExtractArrayRaw(tags_str))::Int32 as tag_id
        from {{ ref("dim_course_names") }}
    )
select course_key, value as tag, course_name, mrt.name as taxonomy_name, lineage, tag_id
from parsed_tags
inner join {{ ref("dim_most_recent_tags") }} mrot FINAL on mrot.id = tag_id
inner join {{ ref("dim_most_recent_taxonomies") }} mrt FINAL on mrt.id = mrot.taxonomy
