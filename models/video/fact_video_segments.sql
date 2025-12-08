{{
    config(
        materialized="materialized_view",
        engine=get_engine("ReplacingMergeTree()"),
        primary_key="(org, course_key, object_id, actor_id, watched_segment)",
        order_by="(org, course_key, object_id, actor_id, watched_segment)",
    )
}}

with
    data as (
        select
            event_id,
            org,
            course_key,
            actor_id,
            emission_time_long,
            video_position,
            object_id,
            video_duration,
            if(
                verb_id = 'https://w3id.org/xapi/video/verbs/played', 'start', 'end'
            ) as verb
        from {{ ref("video_playback_events") }}
        where verb_id <> 'http://adlnet.gov/expapi/verbs/initialized'
    ),
    matches as (
        select
            *,
            first_value(event_id) filter (where verb = 'start') over (
                partition by org, course_key, actor_id, object_id
                order by emission_time_long, verb
                rows between 1 following and unbounded following
            ) as matching_event_id
        from data
    ),
    final_matches as (
        select
            *,
            last_value(event_id) over (
                partition by matching_event_id, object_id, actor_id
                order by emission_time_long
                rows between unbounded preceding and unbounded following
            ) as end_id
        from matches
        order by emission_time_long
    ),
    ends as (select * from final_matches where verb = 'end' and event_id = end_id),
    starts as (select * from final_matches where verb = 'start'),
    joined as (
        select
            starts.event_id as event_id,
            starts.org as org,
            starts.course_key as course_key,
            starts.actor_id as actor_id,
            starts.object_id as object_id,
            starts.video_duration as video_duration,
            arrayJoin(
                range(
                    greatest(cast(starts.video_position as int), 1),
                    cast(ends.video_position as int) + 1,
                    1
                )
            ) as watched_segment
        from starts
        inner join ends on starts.end_id = ends.event_id
        where ends.video_position > starts.video_position
    ),
    final_results as (
        select
            joined.org,
            joined.course_key,
            joined.actor_id,
            joined.object_id,
            joined.video_duration,
            joined.watched_segment,
            count(1) as watch_count,
            {{ format_object_location("blocks.display_name_with_location") }},  -- object_number, object_name_location
            concat(
                '<a href="',
                joined.object_id,
                '" target="_blank">',
                object_name_location,
                '</a>'
            ) as video_link,
            blocks.section_with_name as section_with_name,
            blocks.subsection_with_name as subsection_with_name
        from joined
        join
            {{ ref("dim_course_blocks") }} blocks
            on (
                joined.course_key = blocks.course_key
                and splitByString('/xblock/', joined.object_id)[-1] = blocks.block_id
            )
        group by
            org,
            course_key,
            actor_id,
            object_id,
            video_duration,
            watched_segment,
            object_location,
            object_name_location,
            video_link,
            section_with_name,
            subsection_with_name
    )
select
    org,
    course_key,
    actor_id,
    object_id,
    video_duration,
    watched_segment,
    watch_count,
    object_location as video_location,
    object_name_location as video_name_location,
    video_link,
    section_with_name,
    subsection_with_name
from final_results
