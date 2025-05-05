select
    plays.org as org,
    plays.course_key as course_key,
    items.subsection_course_order as course_order,
    plays.actor_id as actor_id,
    'section' as section_content_level,
    'subsection' as subsection_content_level,
    count(items.original_block_id) as item_count,
    sum(plays.videos_viewed) as videos_viewed,
    items.section_block_id as section_block_id,
    items.subsection_block_id as subsection_block_id,
    items.section_with_name as section_with_name,
    items.subsection_with_name as subsection_with_name
from {{ ref("items_per_subsection") }} items
join
    {{ ref("fact_video_playback_events_played") }} plays
    on (
        items.org = plays.org
        and items.course_key = plays.course_key
        and items.section_number = plays.section_number
        and items.subsection_number = plays.subsection_number
    )
where items.original_block_id like '%@video+block@%'
group by
    org,
    course_key,
    course_order,
    actor_id,
    section_block_id,
    subsection_block_id,
    section_with_name,
    subsection_with_name
