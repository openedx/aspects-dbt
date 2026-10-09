-- Course structure, rebuilt from the latest block data on a timer so reports don't recompute it on
-- every query. Blocks missing from their course's latest publish (deleted) are kept for reports
-- on past activity, flagged with in_latest_publish = false.
{{
    config(
        materialized="materialized_view",
        engine=get_engine("MergeTree()"),
        order_by="(org, course_key, block_id)",
        refreshable={
            "interval": env_var("ASPECTS_COURSE_STRUCTURE_REFRESH", "EVERY 5 MINUTE"),
            "randomize": env_var(
                "ASPECTS_COURSE_STRUCTURE_REFRESH_RANDOMIZE", "1 MINUTE"
            ),
        },
        post_hook=[
            "system refresh view {{ this.schema }}.{{ this.identifier }}_mv",
            "system wait view {{ this.schema }}.{{ this.identifier }}_mv",
        ],
    )
}}

with
    latest_publish as (
        select course_key, max(time_last_dumped) as time_last_dumped
        from {{ ref("dim_most_recent_course_blocks") }}
        group by course_key
    ),
    blocks as (
        select
            courses.org as org,
            courses.course_key as course_key,
            courses.course_name as course_name,
            courses.course_run as course_run,
            blocks.location as block_id,
            blocks.block_name as block_name,
            splitByString(' - ', blocks.display_name_with_location)[
                1
            ] as hierarchy_location,
            splitByString(':', hierarchy_location, 3) as _split_hierarchy,
            concat(
                _split_hierarchy[1], ':', _split_hierarchy[2], ':0'
            ) as subsection_number,
            concat(_split_hierarchy[1], ':0:0') as section_number,
            blocks.display_name_with_location as display_name_with_location,
            blocks.course_order as course_order,
            toBool(blocks.graded) as graded,
            case
                when block_id like '%@chapter+block@%'
                then 'section'
                when block_id like '%@sequential+block@%'
                then 'subsection'
                when block_id like '%@vertical+block@%'
                then 'unit'
                else regexpExtract(block_id, '@([^+]+)\+block@', 1)
            end as block_type,
            toBool(blocks.time_last_dumped = latest_publish.time_last_dumped) as in_latest_publish
        from {{ ref("dim_most_recent_course_blocks") }} blocks
        join {{ ref("dim_course_names") }} courses on blocks.course_key = courses.course_key
        join latest_publish on blocks.course_key = latest_publish.course_key
    ),
    -- Only sections and subsections in the latest publish name the blocks under them, so a
    -- deleted one can't duplicate rows.
    sections as (
        select
            org,
            course_key,
            hierarchy_location,
            block_id,
            display_name_with_location,
            course_order
        from blocks
        where block_type = 'section' and in_latest_publish
    ),
    subsections as (
        select
            org,
            course_key,
            hierarchy_location,
            block_id,
            display_name_with_location,
            course_order
        from blocks
        where block_type = 'subsection' and in_latest_publish
    )
select
    blocks.org as org,
    blocks.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    blocks.block_id as block_id,
    section_blocks.block_id as section_block_id,
    subsection_blocks.block_id as subsection_block_id,
    blocks.block_name as block_name,
    blocks.section_number as section_number,
    blocks.subsection_number as subsection_number,
    blocks.hierarchy_location as hierarchy_location,
    blocks.display_name_with_location as display_name_with_location,
    blocks.course_order as course_order,
    blocks.graded as graded,
    blocks.block_type as block_type,
    section_blocks.display_name_with_location as section_with_name,
    subsection_blocks.display_name_with_location as subsection_with_name,
    section_blocks.course_order as section_course_order,
    subsection_blocks.course_order as subsection_course_order,
    blocks.in_latest_publish as in_latest_publish
from blocks
left join
    sections section_blocks
    on (
        blocks.section_number = section_blocks.hierarchy_location
        and blocks.org = section_blocks.org
        and blocks.course_key = section_blocks.course_key
    )
left join
    subsections subsection_blocks
    on (
        blocks.subsection_number = subsection_blocks.hierarchy_location
        and blocks.org = subsection_blocks.org
        and blocks.course_key = subsection_blocks.course_key
    )
-- `settings final = 1` instead of FINAL, which breaks the unit test fixture.
settings final = 1
