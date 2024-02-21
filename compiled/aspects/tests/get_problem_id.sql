select
    'http://local.edly.io:8000/xblock/block-v1:edunext+demo+demo+ccx+type@problem+block@3c1646f7133a4c5fb4557d649e22c251' as object_id
from
    system.one
where
    
   regexpExtract(object_id, 'xblock/([\w\d-\+:@]*@problem\+block@[\w\d][^_]*)(_\d_\d)?', 1)
 != 'block-v1:edunext+demo+demo+ccx+type@problem+block@3c1646f7133a4c5fb4557d649e22c251'