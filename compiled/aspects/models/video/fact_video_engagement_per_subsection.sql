with
    viewed_subsection_videos as (
        select distinct
            date(emission_time) as viewed_on,
            org,
            course_key,
            course_run,
            
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
            graded,
            actor_id,
            video_id,
            username,
            name,
            email
        from `xapi`.`fact_video_plays`
    )

select
    views.viewed_on,
    views.org,
    views.course_key,
    views.course_run,
    videos.section_with_name,
    videos.subsection_with_name,
    videos.course_order,
    videos.item_count,
    views.actor_id,
    views.video_id,
    views.graded,
    views.username as username,
    views.name as name,
    views.email as email
from viewed_subsection_videos views
join
    `xapi`.`int_videos_per_subsection` videos
    on (
        views.org = videos.org
        and views.course_key = videos.course_key
        and views.section_number = videos.section_number
        and views.subsection_number = videos.subsection_number
    )