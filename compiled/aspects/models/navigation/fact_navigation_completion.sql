with
    pages_per_subsection as (
        select * from (
    with
        items_per_subsection as (
            select
                org,
                course_key,
                section_number,
                subsection_number,
                min(course_order) as course_order,
                graded,
                count(*) as item_count
            from `xapi`.`dim_course_blocks`
            where block_id like '%@vertical+block@%'
            group by org, course_key, section_number, subsection_number, graded
        )

    select
        ips.org as org,
        ips.course_key as course_key,
        ips.section_number as section_number,
        section_blocks.display_name_with_location as section_with_name,
        ips.subsection_number as subsection_number,
        subsection_blocks.display_name_with_location as subsection_with_name,
        ips.course_order as course_order,
        ips.graded as graded,
        ips.item_count as item_count,
        subsection_blocks.block_id as subsection_block_id,
        section_blocks.block_id as section_block_id,
        section_blocks.course_order as section_course_order,
        subsection_blocks.course_order as subsection_course_order
    from items_per_subsection ips
    left join
        `xapi`.`dim_course_blocks` section_blocks
        on (
            ips.section_number = section_blocks.hierarchy_location
            and ips.org = section_blocks.org
            and ips.course_key = section_blocks.course_key
            and section_blocks.block_id like '%@chapter+block@%'
        )
    left join
        `xapi`.`dim_course_blocks` subsection_blocks
        on (
            ips.subsection_number = subsection_blocks.hierarchy_location
            and ips.org = subsection_blocks.org
            and ips.course_key = subsection_blocks.course_key
            and subsection_blocks.block_id like '%@sequential+block@%'
        )
)
    )
select
    navigation.org as org,
    navigation.course_key as course_key,
    navigation.block_id as block_id,
    pages.subsection_course_order as course_order,
    navigation.actor_id as actor_id,
    pages.item_count as page_count,
    pages.section_with_name as section_with_name,
    pages.subsection_with_name as subsection_with_name,
    date(navigation.emission_time) as visited_on
from `xapi`.`navigation_events` navigation
join
    `xapi`.`dim_course_blocks` blocks
    on (
        navigation.course_key = blocks.course_key
        and navigation.block_id = blocks.block_id
    )
join
    pages_per_subsection pages
    on (
        pages.org = navigation.org
        and pages.course_key = navigation.course_key
        and pages.section_number = blocks.section_number
        and pages.subsection_number = blocks.subsection_number
    )