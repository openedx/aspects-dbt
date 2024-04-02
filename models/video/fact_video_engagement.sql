with
    viewed_subsection_videos as (
        select distinct
            date(emission_time) as viewed_on,
            org,
            course_key,
            {{ section_from_display("video_name_with_location") }} as section_number,
            {{ subsection_from_display("video_name_with_location") }}
            as subsection_number,
            actor_id,
            video_id
        from {{ ref("fact_video_plays") }}
    )

select
    views.viewed_on,
    views.org,
    views.course_key,
    videos.section_with_name,
    videos.subsection_with_name,
    videos.item_count,
    views.actor_id,
    views.video_id
from viewed_subsection_videos views
join
    {{ ref("int_videos_per_subsection") }} videos
    on (
        views.org = videos.org
        and views.course_key = videos.course_key
        and views.section_number = videos.section_number
        and views.subsection_number = videos.subsection_number
    )
