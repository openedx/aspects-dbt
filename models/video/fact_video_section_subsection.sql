with 
    fact_videos_per_subsection as (
        select * from ({{ items_per_subsection("%@video+block@%") }})
    )
select
    plays.org as org,
    plays.course_key as course_key,
    videos.subsection_course_order as course_order,
    plays.actor_id as actor_id,
    'section' as section_content_level,
    'subsection' as subsection_content_level,
    videos.item_count as item_count,
    sum(case when plays.videos_viewed = '' then 0 else plays.videos_viewed end) as videos_viewed,
    videos.section_block_id as section_block_id,
    videos.subsection_block_id as subsection_block_id,
    videos.section_with_name as section_with_name,
    videos.subsection_with_name as subsection_with_name
from fact_videos_per_subsection videos
left join
    {{ ref("fact_video_playback_events_played") }} plays
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