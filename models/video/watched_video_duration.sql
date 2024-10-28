{{
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_XAPI_DATABASE", "xapi"),
        engine=get_engine("ReplacingMergeTree()"),
        order_by="(org,course_key,actor_id)",
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster() }} FINAL",
    )
}}

with
    potential_rewatched as (
        select org, course_key, actor_id, video_id
        from {{ ref("watched_video_slices") }}
        group by org, course_key, actor_id, video_id
        having count(1) > 1
    ),
    rewatched_data as (
        select
            org,
            course_key,
            actor_id,
            video_id,
            start_emission_time,
            event_id,
            start_position,
            end_position
        from {{ ref("watched_video_slices") }}
        where
            (org, course_key, actor_id, video_id)
            in (select org, course_key, actor_id, video_id from potential_rewatched)
    ),
    rewatched as (
        select distinct a.event_id as event_id1, b.event_id as event_id2
        from rewatched_data a
        inner join
            rewatched_data b
            on a.org = b.org
            and a.course_key = b.course_key
            and a.actor_id = b.actor_id
            and a.video_id = b.video_id
        where
            b.start_emission_time > a.start_emission_time
            and (
                (
                    b.start_position > a.start_position
                    and b.start_position < a.end_position
                )
                or (
                    b.end_position > a.start_position
                    and b.end_position < a.end_position
                )
            )
    ),
    course_data as (
        select org, course_key, count(distinct block_id) video_count
        from {{ ref("dim_course_blocks") }}
        where block_type = 'video'
        group by org, course_key
    )
select
    course_data.org as org,
    course_data.course_key as course_key,
    range.actor_id as actor_id,
    video_duration,
    cast(video_count as Int32) as video_count,
    sum(
        case when empty(r1.event_id) then end_position - start_position else 0 end
    ) as watched_time,
    sum(
        case when notEmpty(r1.event_id) then end_position - start_position else 0 end
    ) as rewatched_time
from course_data
left join
    {{ ref("watched_video_slices") }} range on range.course_key = course_data.course_key
left join
    (
        select event_id1 as event_id
        from rewatched
        union all
        select event_id2 as event_id
        from rewatched
    ) r1
    on range.event_id = r1.event_id
group by org, course_key, actor_id, video_count, video_duration
