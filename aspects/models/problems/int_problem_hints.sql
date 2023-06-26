-- verb id for showanswer and hint events: http://adlnet.gov/expapi/verbs/asked

select
    emission_time,
    org,
    course_id,
    {{ get_problem_id('object_id') }} as problem_id,
    actor_id,
    case
        when object_id like '%/hint%' then 'hint'
        when object_id like '%/answer%' then 'answer'
        else 'N/A'
    end as help_type
from
    {{ source('xapi', 'problem_events') }}
where
    verb_id = 'http://adlnet.gov/expapi/verbs/asked'
