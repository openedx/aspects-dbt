

with
    fact_video_segments as (
        select
            segments.org as org,
            segments.course_key as course_key,
            blocks.section_number as section_number,
            blocks.subsection_number as subsection_number,
            segments.actor_id as actor_id,
            count(distinct blocks.block_id) as videos_viewed
        from `xapi`.`fact_video_segments` segments
        join
            `xapi`.`dim_course_blocks` blocks
            on (
                segments.course_key = blocks.course_key
                and splitByString('/xblock/', segments.object_id)[-1] = blocks.block_id
            )
        group by org, course_key, section_number, subsection_number, actor_id
    ),
    fact_videos_per_subsection as (
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
            where block_id like '%@video+block@%'
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
    ),
    fact_video_section_subsection as (
        select
            videos.org as org,
            videos.course_key as course_key,
            videos.subsection_course_order as course_order,
            plays.actor_id as actor_id,
            'section' as section_content_level,
            'subsection' as subsection_content_level,
            videos.item_count as item_count,
            sum(plays.videos_viewed) as videos_viewed,
            videos.section_block_id as section_block_id,
            videos.subsection_block_id as subsection_block_id,
            videos.section_with_name as section_with_name,
            videos.subsection_with_name as subsection_with_name
        from fact_video_segments plays
        full join
            fact_videos_per_subsection videos
            on (
                videos.org = plays.org
                and videos.course_key = plays.course_key
                and videos.section_number = plays.section_number
                and videos.subsection_number = plays.subsection_number
            )
        group by
            org,
            course_key,
            course_order,
            actor_id,
            item_count,
            section_block_id,
            subsection_block_id,
            section_with_name,
            subsection_with_name
    ),
    video_engagment as (
        select
            org,
            course_key,
            actor_id,
            sum(videos_viewed) as videos_viewed,
            sum(item_count) as item_count,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as section_subsection_video_engagement,
            block_id,
            section_subsection_name,
            content_level
        from fact_video_section_subsection ARRAY
        join
            arrayConcat([subsection_block_id], [section_block_id]) as block_id,
            arrayConcat(
                [subsection_with_name], [section_with_name]
            ) as section_subsection_name,
            arrayConcat(
                [subsection_content_level], [section_content_level]
            ) as content_level
        group by
            org, course_key, actor_id, block_id, section_subsection_name, content_level
    ),
    final_results as (
        select
            video_engagment.org as org,
            video_engagment.course_key as course_key,
            video_engagment.section_subsection_name as section_subsection_name,
            video_engagment.content_level as content_level,
            video_engagment.actor_id as actor_id,
            video_engagment.section_subsection_video_engagement
            as section_subsection_video_engagement,
            video_engagment.block_id as block_id,
            users.username as username,
            users.name as name,
            users.email as email
        from video_engagment
        left outer join
            `xapi`.`dim_user_pii` users
            on (
                video_engagment.actor_id like 'mailto:%'
                and SUBSTRING(video_engagment.actor_id, 8) = users.email
            )
            or video_engagment.actor_id = toString(users.external_user_id)
        where section_subsection_name <> ''
    )
select *
from final_results