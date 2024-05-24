create materialized view if not exists `xapi`.`subsection_video_engagement_mv` 
  
  to `xapi`.`subsection_video_engagement`
  as 

with
    viewed_subsection_videos as (
        select distinct
            date(emission_time) as viewed_on,
            org,
            course_key,
            
    concat(
        splitByString(
            ':', splitByString(' - ', video_name_with_location)[1], 1
        )[1],
        ':0:0'
    )
 as section_number,
            
    concat(
        arrayStringConcat(
            splitByString(
                ':', splitByString(' - ', video_name_with_location)[1], 2
            ),
            ':'
        ),
        ':0'
    )

            as subsection_number,
            actor_id,
            video_id
        from `xapi`.`fact_video_plays`
    ),
    fact_video_engagement_per_subsection as (
        select
            views.org as org,
            views.course_key as course_key,
            videos.section_with_name as section_with_name,
            videos.subsection_with_name as subsection_with_name,
            videos.item_count as item_count,
            views.actor_id as actor_id,
            views.video_id as video_id,
            videos.subsection_block_id as subsection_block_id
        from viewed_subsection_videos views
        join
            `xapi`.`int_videos_per_subsection` videos
            on (
                views.org = videos.org
                and views.course_key = videos.course_key
                and views.section_number = videos.section_number
                and views.subsection_number = videos.subsection_number
            )
    ),
    subsection_counts as (
        select
            org,
            course_key,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            count(distinct video_id) as videos_viewed,
            case
                when videos_viewed = 0
                then 'No videos viewed yet'
                when videos_viewed = item_count
                then 'All videos viewed'
                else 'At least one video viewed'
            end as engagement_level,
            subsection_block_id
        from fact_video_engagement_per_subsection
        group by
            org,
            course_key,
            section_with_name,
            subsection_with_name,
            actor_id,
            item_count,
            subsection_block_id
    )
select org, course_key, actor_id, subsection_block_id, engagement_level
from subsection_counts