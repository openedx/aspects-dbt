select
    'http://local.edly.io/xblock/block-v1:OpenedX+DemoX+DemoCourse+type@video+block@d5a54ce52f464acfa7a83ae155712cc3' as object_id,
    'The Purpose, Power and Reach of the Open edX® Platform' as video_name,
    
    concat(
        '<a href="', object_id, '" target="_blank">', video_name, '</a>'
    )
 as object_a_tag
from
    system.numbers
where object_a_tag != '<a href="http://local.edly.io/xblock/block-v1:OpenedX+DemoX+DemoCourse+type@video+block@d5a54ce52f464acfa7a83ae155712cc3" target="_blank">The Purpose, Power and Reach of the Open edX® Platform</a>'