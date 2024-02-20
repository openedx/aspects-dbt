select
    'http://local.edly.io:8000/xblock/ccx-block-v1:edunext+demo+demo+ccx@1+type@problem+block@3c1646f7133a4c5fb4557d649e22c251' as object_id
from
    system.one
where
    {{ get_problem_id('object_id') }} != 'ccx-block-v1:edunext+demo+demo+ccx@1+type@problem+block@3c1646f7133a4c5fb4557d649e22c251'
