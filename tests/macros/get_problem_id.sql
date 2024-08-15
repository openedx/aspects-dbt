select
    'http://local.edly.io:8000/xblock/block-v1:edunext+demo+demo+ccx+type@problem+block@3c1646f7133a4c5fb4557d649e22c251'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != 'block-v1:edunext+demo+demo+ccx+type@problem+block@3c1646f7133a4c5fb4557d649e22c251'

union all

select
    'http://localhost:18000/course/course-v1:Org1+DemoX+1937e7'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != ''

union all

select
    'http://localhost:18000/xblock/block-v1:course-v1:Org0+DemoX+2bc51b+type@problem+block@09b07a8d'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != 'block-v1:course-v1:Org0+DemoX+2bc51b+type@problem+block@09b07a8d'

union all

select
    'http://localhost:18000/xblock/block-v1:course-v1:Org0+DemoX+2bc51b+type@problem+block@09b07a8d/answer'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != 'block-v1:course-v1:Org0+DemoX+2bc51b+type@problem+block@09b07a8d'

union all

select
    'http://localhost:18000/xblock/block-v1:course-v1:Org0+DemoX+81bba1+type@problem+block@20d3f709/hint/1'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != 'block-v1:course-v1:Org0+DemoX+81bba1+type@problem+block@20d3f709'

union all

select
    'http://localhost:18000/xblock/block-v1:course-v1:Org0+DemoX+81bba1+type@problem+block@20d3f709/hint/1'
    as object_id
from system.one
where
    {{ get_problem_id("object_id") }}
    != 'block-v1:course-v1:Org0+DemoX+81bba1+type@problem+block@20d3f709'
