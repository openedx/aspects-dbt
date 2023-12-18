select
    transcripts.emission_time as emission_time,
    transcripts.org as org,
    transcripts.course_key as course_key,
    blocks.course_name as course_name,
    blocks.course_run as course_run,
    transcripts.video_id as video_id,
    blocks.block_name as video_name,
    blocks.display_name_with_location as video_name_with_location,
    transcripts.actor_id as actor_id
from
    {{ ref('video_transcript_events') }} transcripts
    join {{ ref('dim_course_blocks')}} blocks
         on (transcripts.course_key = blocks.course_key
             and transcripts.video_id = blocks.block_id)
where
    transcripts.cc_enabled
