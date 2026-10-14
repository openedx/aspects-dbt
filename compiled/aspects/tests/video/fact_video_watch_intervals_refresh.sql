-- Fails if the last fact_video_watch_intervals refresh errored on any replica.



select hostName() as host, view, status, exception
from
     system.view_refreshes
    
where
    database = 'xapi'
    and view = 'fact_video_watch_intervals_mv'
    and exception != ''